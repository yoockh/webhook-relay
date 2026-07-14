-- infrastructure for routing deliveries, not a core requirement.
CREATE TABLE IF NOT EXISTS customers (
    customer_id  TEXT PRIMARY KEY,
    webhook_url  TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Events accepted from internal producers, delivered to the customer endpoint.
CREATE TABLE IF NOT EXISTS events (
    id               UUID PRIMARY KEY,
    customer_id      TEXT NOT NULL REFERENCES customers(customer_id),
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending', -- pending | delivered | failed
    sequence_number  BIGINT NOT NULL,                 -- per-customer monotonic ordering
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_customer_id ON events (customer_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON events (status);

-- Enforces per-customer ordering / de-dup of sequence numbers.
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_customer_seq ON events (customer_id, sequence_number);

-- One row per delivery attempt, forming the audit trail for an event.
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id             UUID PRIMARY KEY,
    event_id       UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    status_code    INT,           -- HTTP status of the attempt, NULL on transport error
    error_message  TEXT,
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_event_id ON delivery_attempts (event_id);
