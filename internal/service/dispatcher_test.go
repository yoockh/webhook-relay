package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/datatypes"

	"webhook-relay/internal/model"
)

// fakeStore is an in-memory deliveryStore so the worker lifecycle can be tested
// without a database.
type fakeStore struct {
	mu       sync.Mutex
	events   []*model.Event
	custs    map[string]string
	attempts []*model.DeliveryAttempt
}

func newFakeStore() *fakeStore { return &fakeStore{custs: map[string]string{}} }

func (s *fakeStore) addEvent(id, customerID string, seq int64) {
	s.mu.Lock()
	s.events = append(s.events, &model.Event{
		ID:             id,
		CustomerID:     customerID,
		EventType:      "test.event",
		SequenceNumber: seq,
		Status:         model.StatusPending,
		Payload:        datatypes.JSON(`{"hello":"world"}`),
	})
	s.mu.Unlock()
}

func (s *fakeStore) setCustomer(id, url string) {
	s.mu.Lock()
	s.custs[id] = url
	s.mu.Unlock()
}

func (s *fakeStore) NextPendingEvent(customerID string) (*model.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *model.Event
	for _, e := range s.events {
		if e.CustomerID == customerID && e.Status == model.StatusPending {
			if best == nil || e.SequenceNumber < best.SequenceNumber {
				best = e
			}
		}
	}
	return best, nil
}

func (s *fakeStore) GetCustomer(customerID string) (*model.Customer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	url, ok := s.custs[customerID]
	if !ok {
		return nil, errors.New("customer not found")
	}
	return &model.Customer{CustomerID: customerID, WebhookURL: url}, nil
}

func (s *fakeStore) UpdateEventStatus(id string, status model.EventStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e.ID == id {
			e.Status = status
		}
	}
	return nil
}

func (s *fakeStore) CreateDeliveryAttempt(a *model.DeliveryAttempt) error {
	s.mu.Lock()
	s.attempts = append(s.attempts, a)
	s.mu.Unlock()
	return nil
}

func (s *fakeStore) status(id string) model.EventStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e.ID == id {
			return e.Status
		}
	}
	return ""
}

func (s *fakeStore) attemptCount(eventID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, a := range s.attempts {
		if a.EventID == eventID {
			n++
		}
	}
	return n
}

// fastPolicy makes every retry wait a couple of milliseconds so tests exercise
// the full attempt count without waiting real minutes.
func fastPolicy() BackoffPolicy {
	return NewBackoffPolicy([]time.Duration{2 * time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond})
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func TestDispatcher_DeliversInOrder(t *testing.T) {
	store := newFakeStore()
	var mu sync.Mutex
	var order []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b deliveryBody
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		order = append(order, b.SequenceNumber)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store.setCustomer("c1", srv.URL)
	for i := int64(1); i <= 5; i++ {
		store.addEvent(idFor(i), "c1", i)
	}

	d := NewDispatcher(store, WithBackoffPolicy(fastPolicy()))
	defer d.Shutdown()
	d.Enqueue("c1")

	if !waitFor(t, 2*time.Second, func() bool { return store.status(idFor(5)) == model.StatusDelivered }) {
		t.Fatal("events not all delivered in time")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []int64{1, 2, 3, 4, 5}
	if len(order) != len(want) {
		t.Fatalf("received %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("delivery order = %v, want %v", order, want)
		}
	}
}

func TestDispatcher_RetriesThenSucceeds(t *testing.T) {
	store := newFakeStore()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 { // fail first two attempts
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store.setCustomer("c1", srv.URL)
	store.addEvent("e1", "c1", 1)

	d := NewDispatcher(store, WithBackoffPolicy(fastPolicy()))
	defer d.Shutdown()
	d.Enqueue("c1")

	if !waitFor(t, 2*time.Second, func() bool { return store.status("e1") == model.StatusDelivered }) {
		t.Fatalf("event not delivered, status = %s", store.status("e1"))
	}
	if got := store.attemptCount("e1"); got != 3 {
		t.Fatalf("attempt count = %d, want 3 (2 failures + 1 success)", got)
	}
}

func TestDispatcher_DeadLettersAfterMaxAttempts(t *testing.T) {
	store := newFakeStore()
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	store.setCustomer("c1", srv.URL)
	store.addEvent("e1", "c1", 1)

	d := NewDispatcher(store, WithBackoffPolicy(fastPolicy()))
	defer d.Shutdown()
	d.Enqueue("c1")

	if !waitFor(t, 2*time.Second, func() bool { return store.status("e1") == model.StatusFailed }) {
		t.Fatalf("event not dead-lettered, status = %s", store.status("e1"))
	}
	if got := atomic.LoadInt32(&calls); got != 5 {
		t.Fatalf("HTTP attempts = %d, want 5", got)
	}
	if got := store.attemptCount("e1"); got != 5 {
		t.Fatalf("recorded attempts = %d, want 5", got)
	}
}

func TestDispatcher_IsolatesCustomers(t *testing.T) {
	store := newFakeStore()

	// customer A is slow: each request blocks well past B's whole delivery.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer slow.Close()
	// customer B is fast and healthy.
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer fast.Close()

	store.setCustomer("A", slow.URL)
	store.setCustomer("B", fast.URL)
	store.addEvent("a1", "A", 1)
	store.addEvent("b1", "B", 1)

	d := NewDispatcher(store, WithBackoffPolicy(fastPolicy()))
	defer d.Shutdown()

	start := time.Now()
	d.Enqueue("A")
	d.Enqueue("B")

	if !waitFor(t, time.Second, func() bool { return store.status("b1") == model.StatusDelivered }) {
		t.Fatal("B was not delivered")
	}
	// B must complete long before A's first (200ms) request even returns,
	// proving A's slow endpoint did not block B's worker.
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("B delivery took %v, want < 150ms (A appears to be blocking B)", elapsed)
	}
}

func TestDispatcher_SendsEnvelopeAndIdempotencyKey(t *testing.T) {
	store := newFakeStore()
	type captured struct {
		body      deliveryBody
		idemKey   string
		eventIDHd string
	}
	ch := make(chan captured, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b deliveryBody
		_ = json.NewDecoder(r.Body).Decode(&b)
		ch <- captured{body: b, idemKey: r.Header.Get("X-Idempotency-Key"), eventIDHd: r.Header.Get("X-Webhook-Event-Id")}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store.setCustomer("c1", srv.URL)
	store.addEvent("evt-123", "c1", 7)

	d := NewDispatcher(store, WithBackoffPolicy(fastPolicy()))
	defer d.Shutdown()
	d.Enqueue("c1")

	select {
	case c := <-ch:
		if c.body.EventID != "evt-123" || c.body.SequenceNumber != 7 || c.body.EventType != "test.event" {
			t.Fatalf("envelope = %+v, want event_id=evt-123 seq=7", c.body)
		}
		if string(c.body.Payload) != `{"hello":"world"}` {
			t.Fatalf("payload = %s, want original JSON", c.body.Payload)
		}
		if c.idemKey != "evt-123" || c.eventIDHd != "evt-123" {
			t.Fatalf("idempotency headers = %q/%q, want evt-123", c.idemKey, c.eventIDHd)
		}
	case <-time.After(time.Second):
		t.Fatal("no delivery received")
	}
}

func TestDispatcher_ShutdownInterruptsBackoff(t *testing.T) {
	store := newFakeStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // always fails -> enters backoff
	}))
	defer srv.Close()

	store.setCustomer("c1", srv.URL)
	store.addEvent("e1", "c1", 1)

	// Long backoff so the worker is parked in the wait when we shut down.
	slow := NewBackoffPolicy([]time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second, 10 * time.Second})
	d := NewDispatcher(store, WithBackoffPolicy(slow))
	d.Enqueue("c1")

	// Let the first attempt happen and the worker enter its backoff wait.
	waitFor(t, time.Second, func() bool { return store.attemptCount("e1") >= 1 })

	done := make(chan struct{})
	go func() { d.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return promptly; backoff wait was not interrupted")
	}
	if store.status("e1") == model.StatusFailed {
		t.Fatal("event was dead-lettered; shutdown should have left it pending mid-backoff")
	}
}

func idFor(n int64) string {
	return "evt-" + string(rune('0'+n))
}
