package repository

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"webhook-relay/internal/model"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("record not found")

// Repository is the data-access layer for events, delivery attempts and
// customers. It is the only place that talks to the database.
type Repository struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// UpsertCustomer creates or updates a customer's webhook URL.
func (r *Repository) UpsertCustomer(customerID, webhookURL string) (*model.Customer, error) {
	c := model.Customer{CustomerID: customerID, WebhookURL: webhookURL}
	err := r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "customer_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"webhook_url", "updated_at"}),
	}).Create(&c).Error
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// GetCustomer returns a customer by id, or ErrNotFound.
func (r *Repository) GetCustomer(customerID string) (*model.Customer, error) {
	var c model.Customer
	err := r.db.First(&c, "customer_id = ?", customerID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// CreateEvent persists a new event, assigning the next per-customer sequence
// number atomically so ordering is well-defined even under concurrent inserts.
func (r *Repository) CreateEvent(event *model.Event) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		var maxSeq *int64
		if err := tx.Model(&model.Event{}).
			Where("customer_id = ?", event.CustomerID).
			Select("MAX(sequence_number)").
			Scan(&maxSeq).Error; err != nil {
			return err
		}
		if maxSeq == nil {
			event.SequenceNumber = 1
		} else {
			event.SequenceNumber = *maxSeq + 1
		}
		if event.Status == "" {
			event.Status = model.StatusPending
		}
		return tx.Create(event).Error
	})
}

// GetEventByID returns an event with its delivery-attempt history, or ErrNotFound.
func (r *Repository) GetEventByID(id string) (*model.Event, error) {
	var e model.Event
	err := r.db.
		Preload("DeliveryAttempts", func(db *gorm.DB) *gorm.DB {
			return db.Order("attempt_number ASC")
		}).
		First(&e, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// ListEvents returns events filtered by optional customerID and status,
// ordered by customer then sequence number. Empty filters are ignored.
func (r *Repository) ListEvents(customerID string, status string) ([]model.Event, error) {
	var events []model.Event
	q := r.db.Model(&model.Event{})
	if customerID != "" {
		q = q.Where("customer_id = ?", customerID)
	}
	if status != "" {
		q = q.Where("status = ?", status)
	}
	err := q.Order("customer_id ASC, sequence_number ASC").Find(&events).Error
	return events, err
}

// NextPendingEvent returns the lowest-sequence pending event for a customer,
// or (nil, nil) when the customer has no pending work. This is how the worker
// honours strict per-customer ordering with sequence_number as the source of
// truth.
func (r *Repository) NextPendingEvent(customerID string) (*model.Event, error) {
	var e model.Event
	err := r.db.
		Where("customer_id = ? AND status = ?", customerID, model.StatusPending).
		Order("sequence_number ASC").
		First(&e).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// CustomersWithPendingEvents returns the distinct customer ids that currently
// have pending events. Useful for re-arming workers (e.g. after restart).
func (r *Repository) CustomersWithPendingEvents() ([]string, error) {
	var ids []string
	err := r.db.Model(&model.Event{}).
		Where("status = ?", model.StatusPending).
		Distinct().
		Pluck("customer_id", &ids).Error
	return ids, err
}

// UpdateEventStatus sets an event's status.
func (r *Repository) UpdateEventStatus(id string, status model.EventStatus) error {
	return r.db.Model(&model.Event{}).
		Where("id = ?", id).
		Update("status", status).Error
}

// CreateDeliveryAttempt records a single delivery attempt.
func (r *Repository) CreateDeliveryAttempt(attempt *model.DeliveryAttempt) error {
	return r.db.Create(attempt).Error
}
