package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// EventStatus is the lifecycle state of an event.
type EventStatus string

const (
	StatusPending   EventStatus = "pending"
	StatusDelivered EventStatus = "delivered"
	StatusFailed    EventStatus = "failed"
)

// Customer owns a webhook endpoint that events are delivered to.
type Customer struct {
	CustomerID string `gorm:"primaryKey" json:"customer_id"`
	WebhookURL string `gorm:"not null" json:"webhook_url"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Event is a message from an internal producer destined for a customer endpoint.
type Event struct {
	ID             string         `gorm:"primaryKey" json:"id"`
	CustomerID     string         `gorm:"index;not null" json:"customer_id"`
	EventType      string         `gorm:"not null" json:"event_type"`
	Payload        datatypes.JSON `gorm:"type:jsonb" json:"payload"`
	Status         EventStatus    `gorm:"index;not null;default:pending" json:"status"`
	SequenceNumber int64          `gorm:"not null" json:"sequence_number"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`

	DeliveryAttempts []DeliveryAttempt `gorm:"foreignKey:EventID" json:"delivery_attempts,omitempty"`
}

// DeliveryAttempt records a single attempt to deliver an event, forming an
// audit trail across retries.
type DeliveryAttempt struct {
	ID            string    `gorm:"primaryKey" json:"id"`
	EventID       string    `gorm:"index;not null" json:"event_id"`
	AttemptNumber int       `gorm:"not null" json:"attempt_number"`
	StatusCode    *int      `json:"status_code,omitempty"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	AttemptedAt   time.Time `json:"attempted_at"`
}

// BeforeCreate assigns a UUID primary key if one was not set.
func (e *Event) BeforeCreate(tx *gorm.DB) error {
	if e.ID == "" {
		e.ID = uuid.NewString()
	}
	return nil
}

// BeforeCreate assigns a UUID primary key if one was not set.
func (d *DeliveryAttempt) BeforeCreate(tx *gorm.DB) error {
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	return nil
}
