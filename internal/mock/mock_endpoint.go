// Package mock provides a configurable stand-in for a customer's webhook
// endpoint. It is a manual-testing aid for exercising the relay's delivery,
// retry, and worker-isolation logic — not part of the production surface.
package mock

import (
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
)

// Delivery is one received webhook, logged so you can eyeball ordering and
// at-least-once dedupe during manual testing.
type Delivery struct {
	EventID        string    `json:"event_id"`
	SequenceNumber int64     `json:"sequence_number"`
	EventType      string    `json:"event_type"`
	Mode           string    `json:"mode"`
	ReceivedAt     time.Time `json:"received_at"`
}

// Endpoint is a mock receiver whose behaviour is chosen per request via the
// "mode" query parameter, so a single instance can back several simulated
// customers at once (e.g. one URL with ?mode=fail, another with ?mode=success).
type Endpoint struct {
	mu       sync.Mutex
	received []Delivery
	downFrom time.Time // when "down" mode was first hit (for recover_after)
}

// New returns an empty mock endpoint.
func New() *Endpoint { return &Endpoint{} }

// Register mounts the mock routes on the given Echo instance:
//
//	ANY  /mock        receive a delivery; behaviour set by ?mode=
//	GET  /mock/log    return everything received so far (JSON)
//	POST /mock/reset  clear the log and recovery state
func (e *Endpoint) Register(app *echo.Echo) {
	app.Any("/mock", e.receive)
	app.GET("/mock/log", e.log)
	app.POST("/mock/reset", e.reset)
}

// receive handles an incoming delivery. Supported modes:
//
//	success (default) -> 200
//	fail              -> 500
//	timeout           -> sleep past the relay's client timeout, then 200.
//	                     Override the sleep with ?delay=<duration> (e.g. 15s).
//	down              -> 503 ("down for an extended period"). With
//	                     ?recover_after=<duration> it returns 503 until that
//	                     much time has passed since the first "down" hit, then
//	                     starts returning 200 — models an outage that heals.
func (e *Endpoint) receive(c echo.Context) error {
	mode := c.QueryParam("mode")
	if mode == "" {
		mode = "success"
	}

	// Parse the delivery envelope sent by the dispatcher (best-effort).
	var body struct {
		EventID        string `json:"event_id"`
		EventType      string `json:"event_type"`
		SequenceNumber int64  `json:"sequence_number"`
	}
	_ = c.Bind(&body)

	switch mode {
	case "fail":
		return c.JSON(http.StatusInternalServerError, echo.Map{"mode": mode, "ok": false})

	case "timeout":
		delay := 15 * time.Second
		if d, err := time.ParseDuration(c.QueryParam("delay")); err == nil {
			delay = d
		}
		time.Sleep(delay)
		e.record(body.EventID, body.SequenceNumber, body.EventType, mode)
		return c.JSON(http.StatusOK, echo.Map{"mode": mode, "ok": true})

	case "down":
		if recovered := e.downRecovered(c.QueryParam("recover_after")); !recovered {
			return c.JSON(http.StatusServiceUnavailable, echo.Map{"mode": mode, "ok": false})
		}
		e.record(body.EventID, body.SequenceNumber, body.EventType, mode)
		return c.JSON(http.StatusOK, echo.Map{"mode": mode, "ok": true, "recovered": true})

	default: // success
		e.record(body.EventID, body.SequenceNumber, body.EventType, mode)
		return c.JSON(http.StatusOK, echo.Map{"mode": "success", "ok": true})
	}
}

// downRecovered reports whether a "down" endpoint should now be treated as
// recovered, given an optional recover_after duration. Without recover_after it
// stays down forever.
func (e *Endpoint) downRecovered(recoverAfter string) bool {
	dur, err := time.ParseDuration(recoverAfter)
	if err != nil || dur <= 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.downFrom.IsZero() {
		e.downFrom = time.Now()
	}
	return time.Since(e.downFrom) >= dur
}

func (e *Endpoint) record(eventID string, seq int64, eventType, mode string) {
	e.mu.Lock()
	e.received = append(e.received, Delivery{
		EventID:        eventID,
		SequenceNumber: seq,
		EventType:      eventType,
		Mode:           mode,
		ReceivedAt:     time.Now().UTC(),
	})
	e.mu.Unlock()
}

func (e *Endpoint) log(c echo.Context) error {
	e.mu.Lock()
	out := make([]Delivery, len(e.received))
	copy(out, e.received)
	e.mu.Unlock()
	return c.JSON(http.StatusOK, out)
}

func (e *Endpoint) reset(c echo.Context) error {
	e.mu.Lock()
	e.received = nil
	e.downFrom = time.Time{}
	e.mu.Unlock()
	return c.NoContent(http.StatusNoContent)
}
