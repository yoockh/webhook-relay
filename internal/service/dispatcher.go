package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"webhook-relay/internal/model"
	"webhook-relay/internal/repository"
)

// deliveryStore is the slice of the repository the dispatcher depends on. Kept
// as an interface so the worker logic can be unit tested without a real DB.
type deliveryStore interface {
	NextPendingEvent(customerID string) (*model.Event, error)
	GetCustomer(customerID string) (*model.Customer, error)
	UpdateEventStatus(id string, status model.EventStatus) error
	CreateDeliveryAttempt(attempt *model.DeliveryAttempt) error
}

// Dispatcher owns one goroutine + notification channel per customer. A slow or
// downed customer endpoint only ever blocks that customer's own worker, so
// other customers' deliveries are unaffected (isolation). Within a customer,
// events are delivered strictly one at a time in sequence_number order
// (ordering).
type Dispatcher struct {
	store  deliveryStore
	policy BackoffPolicy
	client *http.Client

	mu      sync.Mutex
	workers map[string]*worker
	wg      sync.WaitGroup
	quit    chan struct{}
	stopped bool
}

// Option configures a Dispatcher.
type Option func(*Dispatcher)

// WithBackoffPolicy overrides the retry schedule (tests pass a fast one).
func WithBackoffPolicy(p BackoffPolicy) Option {
	return func(d *Dispatcher) { d.policy = p }
}

// WithHTTPClient overrides the HTTP client used to POST to endpoints.
func WithHTTPClient(c *http.Client) Option {
	return func(d *Dispatcher) { d.client = c }
}

// NewDispatcher builds a dispatcher over the given store.
func NewDispatcher(store deliveryStore, opts ...Option) *Dispatcher {
	d := &Dispatcher{
		store:   store,
		policy:  NewBackoffPolicy(nil),
		client:  &http.Client{Timeout: 10 * time.Second},
		workers: make(map[string]*worker),
		quit:    make(chan struct{}),
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Enqueue signals the customer's worker that there may be new work, creating
// the worker on first use. It never blocks: the actual event was already
// persisted, and the worker pulls pending events from the store in order.
func (d *Dispatcher) Enqueue(customerID string) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	w, ok := d.workers[customerID]
	if !ok {
		w = &worker{customerID: customerID, notify: make(chan struct{}, 1), d: d}
		d.workers[customerID] = w
		d.wg.Add(1)
		go w.run()
	}
	d.mu.Unlock()

	// Non-blocking signal. If one is already queued, the worker's drain loop
	// will pick up every pending event anyway, so a dropped signal is safe.
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// Shutdown stops all workers and waits for in-flight attempts to unwind. A
// worker sleeping between retries wakes immediately; it does not finish the
// remaining retries of the event it was on.
func (d *Dispatcher) Shutdown() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	close(d.quit)
	d.mu.Unlock()
	d.wg.Wait()
}

// worker is a single customer's dedicated goroutine.
type worker struct {
	customerID string
	notify     chan struct{}
	d          *Dispatcher
}

func (w *worker) run() {
	defer w.d.wg.Done()
	for {
		select {
		case <-w.d.quit:
			return
		case <-w.notify:
			w.drain()
		}
	}
}

// drain delivers every currently-pending event for the customer, in sequence
// order, one at a time, until none remain or the dispatcher shuts down.
func (w *worker) drain() {
	for {
		select {
		case <-w.d.quit:
			return
		default:
		}
		event, err := w.d.store.NextPendingEvent(w.customerID)
		if err != nil {
			log.Printf("dispatcher: customer=%s next pending: %v", w.customerID, err)
			return
		}
		if event == nil {
			return
		}
		w.deliver(event)
	}
}

// deliver runs the full attempt+retry lifecycle for a single event until it
// reaches a terminal state (delivered or failed). It returns early only on
// shutdown, leaving the event pending for the next run.
func (w *worker) deliver(event *model.Event) {
	customer, err := w.d.store.GetCustomer(event.CustomerID)
	webhookURL := ""
	if err != nil {
		// No endpoint registered yet: treat as a failing attempt so the event
		// retries (and may succeed once the endpoint is registered) rather than
		// silently stalling.
		log.Printf("dispatcher: customer=%s no endpoint: %v", event.CustomerID, err)
	} else {
		webhookURL = customer.WebhookURL
	}

	attemptsMade := 0
	for {
		attemptsMade++
		statusCode, deliverErr := w.attempt(webhookURL, event)
		w.recordAttempt(event.ID, attemptsMade, statusCode, deliverErr)

		if deliverErr == nil && statusCode >= 200 && statusCode < 300 {
			if err := w.d.store.UpdateEventStatus(event.ID, model.StatusDelivered); err != nil {
				log.Printf("dispatcher: event=%s mark delivered: %v", event.ID, err)
			}
			return
		}

		delay, retry := w.d.policy.NextDelay(attemptsMade)
		if !retry {
			if err := w.d.store.UpdateEventStatus(event.ID, model.StatusFailed); err != nil {
				log.Printf("dispatcher: event=%s mark failed: %v", event.ID, err)
			}
			return
		}

		// Interruptible backoff wait. Only this customer's worker is blocked.
		select {
		case <-time.After(delay):
		case <-w.d.quit:
			return
		}
	}
}

// attempt performs a single HTTP POST to the endpoint. It returns the HTTP
// status code (0 when no response was received) and a transport error if any.
func (w *worker) attempt(webhookURL string, event *model.Event) (int, error) {
	if webhookURL == "" {
		return 0, fmt.Errorf("no endpoint registered for customer %s", event.CustomerID)
	}

	body, err := json.Marshal(deliveryBody{
		EventID:        event.ID,
		EventType:      event.EventType,
		SequenceNumber: event.SequenceNumber,
		Payload:        json.RawMessage(event.Payload),
	})
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	// event_id doubles as the idempotency key for at-least-once dedupe.
	req.Header.Set("X-Webhook-Event-Id", event.ID)
	req.Header.Set("X-Idempotency-Key", event.ID)

	resp, err := w.d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// deliveryBody is the JSON envelope POSTed to a customer endpoint.
type deliveryBody struct {
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	SequenceNumber int64           `json:"sequence_number"`
	Payload        json.RawMessage `json:"payload"`
}

func (w *worker) recordAttempt(eventID string, attemptNumber, statusCode int, deliverErr error) {
	att := &model.DeliveryAttempt{
		EventID:       eventID,
		AttemptNumber: attemptNumber,
		AttemptedAt:   time.Now().UTC(),
	}
	if statusCode > 0 {
		att.StatusCode = &statusCode
	}
	if deliverErr != nil {
		att.ErrorMessage = deliverErr.Error()
	} else if statusCode < 200 || statusCode >= 300 {
		att.ErrorMessage = fmt.Sprintf("non-2xx status: %d", statusCode)
	}
	if err := w.d.store.CreateDeliveryAttempt(att); err != nil {
		log.Printf("dispatcher: event=%s record attempt: %v", eventID, err)
	}
}

// ensure repository.Repository satisfies deliveryStore.
var _ deliveryStore = (*repository.Repository)(nil)
