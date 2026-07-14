# Design Decisions

This document explains the four core delivery guarantees and what was deliberately left out of scope.

## 1. Delivery semantics — at-least-once

Delivery is **at-least-once**, not exactly-once. A delivery can succeed on the customer's side while the acknowledgement back to the relay is lost (timeout, dropped connection), in which case the event is retried and the customer receives it again. Guaranteeing exactly-once across an untrusted network is not practically achievable, so instead every delivery carries a unique **`event_id`** — sent both in the JSON envelope and as an `X-Idempotency-Key` header. Customers use it as an idempotency key to dedupe on their end. This pushes the cheap, reliable part of the problem (dedup on a key) to where it can actually be solved.

## 2. Retry — capped exponential backoff

Failed deliveries retry on a fixed, capped schedule: **30s → 2m → 10m → 1h**. Combined with the one immediate first attempt, that is **5 total attempts** (1 + 4 retries). A non-2xx response or a transport error counts as a failure; every attempt is recorded in `delivery_attempts` (status code or error message) for auditability. Once the schedule is exhausted the event is **dead-lettered** (`status = failed`) with no further retries — bounding retries protects both the relay and the customer's endpoint from unbounded load, while the backoff gives a struggling endpoint room to recover. The schedule is a pure, injectable policy (`service/retry.go`) so it is trivial to unit-test and to tune.

## 3. Isolation — one worker per customer

Each customer gets a **dedicated goroutine worker and notification channel**. Backoff waits happen *inside* that worker, so a customer with a slow or downed endpoint only ever blocks their own queue — every other customer keeps delivering unaffected. Workers are created lazily on first event and the channel is a non-blocking "work available" signal, so accepting an event never blocks the producer regardless of how backed-up any customer is.

## 4. Ordering — strict per customer, DB is source of truth

Events are delivered in **strict order per `customer_id`**, one at a time: the worker only advances to the next event after the current one reaches a terminal state (`delivered` or `failed`). Ordering is anchored to the database, not to channel arrival order — each event is assigned a monotonic per-customer **`sequence_number`** at creation (atomically), and the worker always pulls the lowest-`sequence_number` pending event. This makes ordering correct even under concurrent `POST /events` for the same customer, where in-memory arrival order could otherwise diverge from the intended sequence.

## Deliberately out of scope

- **No auto-recovery on restart.** Workers are in-memory; a restart does not automatically re-arm delivery for events left `pending`. The design makes recovery cheap to add later (a `CustomersWithPendingEvents` query + re-enqueue on boot exists), but it is intentionally not wired in.
- **No strict cross-customer ordering.** Ordering is guaranteed *per customer* only. Events from different customers interleave freely — a global total order is neither required nor provided.
- **No auth, UI, multi-tenant billing, or production deployment config.** The focus is the delivery engine (reliability, retries, isolation, ordering); these surrounding concerns are explicitly excluded.
