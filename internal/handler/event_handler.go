package handler

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"

	"webhook-relay/internal/model"
	"webhook-relay/internal/repository"
)

// Enqueuer signals a customer's worker that new work is available. Kept as an
// interface so handlers can be tested without a live dispatcher.
type Enqueuer interface {
	Enqueue(customerID string)
}

// EventHandler wires the HTTP surface to the repository and dispatcher.
type EventHandler struct {
	repo       *repository.Repository
	dispatcher Enqueuer
}

func New(repo *repository.Repository, dispatcher Enqueuer) *EventHandler {
	return &EventHandler{repo: repo, dispatcher: dispatcher}
}

// Register mounts the four endpoints on the Echo instance.
func (h *EventHandler) Register(app *echo.Echo) {
	app.POST("/events", h.CreateEvent)
	app.GET("/events/:event_id", h.GetEvent)
	app.GET("/events", h.ListEvents)
	app.POST("/customers/:customer_id/endpoint", h.RegisterEndpoint)
}

type createEventRequest struct {
	CustomerID string          `json:"customer_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"`
}

type createEventResponse struct {
	EventID        string            `json:"event_id"`
	Status         model.EventStatus `json:"status"`
	SequenceNumber int64             `json:"sequence_number"`
}

// CreateEvent accepts an event from an internal producer, persists it as
// pending, hands it to the customer's queue, and returns immediately without
// waiting for delivery.
func (h *EventHandler) CreateEvent(c echo.Context) error {
	var req createEventRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}
	if req.CustomerID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "customer_id is required")
	}
	if req.EventType == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "event_type is required")
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	} else if !json.Valid(payload) {
		return echo.NewHTTPError(http.StatusBadRequest, "payload must be valid JSON")
	}

	// Ensure the customer exists so the event's FK holds even if the endpoint
	// has not been registered yet.
	if err := h.repo.EnsureCustomer(req.CustomerID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to ensure customer")
	}

	event := &model.Event{
		CustomerID: req.CustomerID,
		EventType:  req.EventType,
		Payload:    []byte(payload),
		Status:     model.StatusPending,
	}
	if err := h.repo.CreateEvent(event); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to create event")
	}

	// Hand off to the per-customer worker and return without blocking.
	h.dispatcher.Enqueue(event.CustomerID)

	return c.JSON(http.StatusAccepted, createEventResponse{
		EventID:        event.ID,
		Status:         event.Status,
		SequenceNumber: event.SequenceNumber,
	})
}

// GetEvent returns an event's current status and its delivery-attempt history.
func (h *EventHandler) GetEvent(c echo.Context) error {
	id := c.Param("event_id")
	event, err := h.repo.GetEventByID(id)
	if err == repository.ErrNotFound {
		return echo.NewHTTPError(http.StatusNotFound, "event not found")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to load event")
	}
	return c.JSON(http.StatusOK, event)
}

// ListEvents lists events, optionally filtered by customer_id and status.
func (h *EventHandler) ListEvents(c echo.Context) error {
	customerID := c.QueryParam("customer_id")
	status := c.QueryParam("status")
	if status != "" && !validStatus(status) {
		return echo.NewHTTPError(http.StatusBadRequest, "status must be one of: pending, delivered, failed")
	}
	events, err := h.repo.ListEvents(customerID, status)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to list events")
	}
	return c.JSON(http.StatusOK, events)
}

type registerEndpointRequest struct {
	WebhookURL string `json:"webhook_url"`
}

// RegisterEndpoint sets (or updates) a customer's webhook URL, then nudges the
// worker in case events were waiting on a missing endpoint.
func (h *EventHandler) RegisterEndpoint(c echo.Context) error {
	customerID := c.Param("customer_id")
	var req registerEndpointRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid JSON body")
	}
	if req.WebhookURL == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "webhook_url is required")
	}

	customer, err := h.repo.UpsertCustomer(customerID, req.WebhookURL)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to register endpoint")
	}

	h.dispatcher.Enqueue(customerID)
	return c.JSON(http.StatusOK, customer)
}

func validStatus(s string) bool {
	switch model.EventStatus(s) {
	case model.StatusPending, model.StatusDelivered, model.StatusFailed:
		return true
	default:
		return false
	}
}
