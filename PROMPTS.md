# AI Prompts
 
This file documents the prompts I found most useful while building this project, as suggested in the candidate brief.
 
## Initial planning prompt
 
Before writing any code, I researched AccelByte's engineering context (backend stack, product domain) and worked through the four required design decisions (delivery semantics, retry behavior, long-outage isolation, and ordering) on my own first. Once I had a clear architecture in mind, I turned that into the following structured prompt for Claude Code, laying out the tech stack, finalized design decisions, file structure, build order, and commit/reporting conventions I wanted it to follow — so the AI was executing a plan I had already reasoned through, rather than making the architectural decisions itself.

---
## Project: webhook-relay

## Context
This is a technical test project for a Junior Software Engineer role. The goal is to build a webhook delivery service: a system that accepts events from internal producers and reliably delivers them to customer-registered HTTP endpoints, with retry logic, delivery status tracking, and per-customer ordering guarantees.

## Tech Stack
- Language: Go
- Web framework: Echo
- ORM: GORM
- Database: PostgreSQL (or SQLite if simpler for local dev, use GORM so it's swappable)
- No auth, no UI, no multi-tenant billing, no production deployment config (out of scope)

## Design Decisions (already finalized, implement exactly this)
1. **Delivery semantics**: At-least-once delivery. Every event includes a unique `event_id` so customers can dedupe on their end (idempotency key).
2. **Retry behavior**: Exponential backoff with capped attempts. Schedule: 30s, 2m, 10m, 1h. After the final attempt fails, mark event status as `failed` (dead-letter, no further retries).
3. **Long outages / isolation**: Each customer gets its own dedicated goroutine worker and channel. A customer with a downed endpoint only blocks their own queue — other customers' deliveries are unaffected.
4. **Ordering**: Strict ordering is guaranteed per customer_id. Each customer's worker processes events one at a time, in sequence — the next event only starts after the current one reaches a terminal state (delivered or failed).

## Database Schema
Two tables:
- `events`: id (UUID), customer_id, event_type, payload (JSONB), status (pending/delivered/failed), sequence_number, created_at, updated_at
- `delivery_attempts`: id (UUID), event_id (FK), attempt_number, status_code, error_message, attempted_at

Also need a simple way to register a customer's webhook_url (a `customers` table or config is fine — keep it minimal, this is supporting infrastructure, not a core requirement).

## Endpoints
1. `POST /events` — accept new event from internal producer, save as pending, push to customer's per-customer queue, return immediately (event_id, status) without waiting for delivery.
2. `GET /events/{event_id}` — return event status and its delivery_attempts history.
3. `GET /events?customer_id=&status=` — list/filter events.
4. `POST /customers/{customer_id}/endpoint` — register a customer's webhook_url.

## Mocking
Include a small mock customer endpoint handler (`internal/mock/mock_endpoint.go`) that can simulate success, failure, timeout, and "down for extended period" — used for manually testing the retry and worker isolation logic.

## File Structure
```
webhook-relay/
├── cmd/server/main.go
├── internal/
│   ├── config/config.go
│   ├── handler/event_handler.go (+ _test.go)
│   ├── model/event.go
│   ├── repository/event_repository.go
│   ├── service/
│   │   ├── dispatcher.go       # per-customer queue + worker manager
│   │   ├── retry.go            # exponential backoff logic
│   │   └── dispatcher_test.go
│   └── mock/mock_endpoint.go
├── migrations/schema.sql
├── DECISIONS.md
├── go.mod / go.sum
└── README.md
```

## Order of Work
Please work in this order, and commit after each step (see commit rules below):
1. `go.mod` init + Echo + GORM setup, `cmd/server/main.go` with a bare health-check route
2. `migrations/schema.sql` — define the two tables above
3. `internal/model/event.go` — GORM structs matching the schema
4. `internal/repository/event_repository.go` — DB access layer (create event, update status, list, get by id, create delivery attempt)
5. `internal/service/retry.go` — backoff schedule logic (pure function, easy to unit test)
6. `internal/service/dispatcher.go` — per-customer channel/worker manager, the core delivery logic
7. `internal/mock/mock_endpoint.go` — mock customer endpoint for manual testing
8. `internal/handler/event_handler.go` — wire up the 4 endpoints
9. Unit tests for retry.go and dispatcher.go (focus here, not blanket coverage)
10. `README.md` with a one-line run command
11. `DECISIONS.md` — half a page covering the 4 decisions above and anything deliberately left out (e.g. no persistence-based recovery on server restart)

## Commit Rules
- Commit after each completed step above. Do NOT push, just commit locally.
- Commit messages: short, professional, conventional-commit style. One line. No long bodies.
  - Good: `feat: add event and delivery_attempts schema`
  - Good: `feat: implement per-customer dispatcher with ordered retry`
  - Bad: long multi-paragraph commit messages explaining everything you did
- If a commit message would naturally need more explanation, keep that explanation in your chat response to me instead, not in the commit body.

## Reporting
After each step (or logically grouped set of steps), stop and give me a short report:
- What was implemented
- Any deviation from the plan above and why
- Any open question or trade-off you made a judgment call on

I'll take your report and decide the next instruction, so don't auto-continue to the next step without my go-ahead.