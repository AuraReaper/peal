# Peal

**Ring once. Reach everywhere.**

[![Go](https://img.shields.io/badge/go-1.22%2B-00ADD8)](https://go.dev)
![status](https://img.shields.io/badge/status-in%20development-orange)

Peal is a small distributed notification service written in Go. A producer submits a notification once; Peal stores it durably, fans it out to every requested channel, and keeps retrying each delivery until it succeeds or is declared dead. It uses only the Go standard library.

It is also a learning project: a hands-on way to practise concurrency, worker coordination, RPC, write-ahead logging and failure handling in Go, in the same style as a coordinator/worker distributed system.

> **Status:** in development. This README describes the target design; the checklist under [Roadmap](#roadmap) shows what exists so far. Command-line flags below are provisional.

## Features

- **Multi-channel delivery:** `console`, `email`, `sms` and `webhook` adapters behind one `Sender` interface. Email and SMS talk to a bundled fake provider, so there are no accounts or API keys.
- **At-least-once delivery:** nothing acknowledged is lost. Idempotency keys on submit and fencing tokens on leases keep duplicates rare and explainable.
- **Lease-based workers:** stateless workers pull jobs from the broker, renew leases with heartbeats, and a worker that dies simply stops renewing.
- **Retries and dead letters:** exponential backoff with full jitter, per-request attempt limits, a dead-letter list, and manual requeue.
- **Scheduled delivery:** `deliver_at` up to 30 days ahead.
- **Resilience:** per-channel token-bucket rate limits and circuit breakers on each worker, and backpressure (HTTP 429) when the queue is full.
- **Durable state:** an append-only write-ahead log with group commit, crash recovery, and snapshots.
- **Observable:** structured `slog` logs, `expvar` counters, and `pprof`.

## How it works

```
                      +-------------------- Broker (owns all state) --------------------+
  Producer  --HTTP--> |  HTTP API        RPC server <-----------------+                  |
  (any client)        |      \            /                          |                  |
                      |       v          v                           |                  |
                      |     State machine: deliveries, ready lists,  |                  |
                      |     delay heap, lease table                  |                  |
                      |       ^                      |               |                  |
                      |   Scheduler                  v               |                  |
                      |  (retry timers,         Log (WAL)            |                  |
                      |   lease reaper)     group commit, fsync      |                  |
                      +----------------------------------------------|------------------+
                                                                     | RPC (workers call in)
                                  +----------------------------------+-----------------+
                                  |  Worker x N (stateless)                            |
                                  |  lease loop -> runner pool -> channel adapters ----+--> Sink / providers
                                  +----------------------------------------------------+
```

1. A producer `POST`s a notification to the broker.
2. The broker validates it, writes a `Created` record to the log, and replies `202` once the record is durable.
3. Workers long-poll `Lease`, receive jobs with a lease token, send through the channel adapter, then call `Complete` or `Fail` with that token.
4. Failures are retried with backoff; permanent failures and exhausted attempts become `dead`.
5. If a worker dies, its leases expire and the deliveries are requeued.

The broker is a deterministic state machine: every change is a record, and state is `Apply(record)` run over the log in order. Replaying the log after a crash rebuilds identical state.

### Delivery states

```
submit -> Queued --lease--> Leased --complete--> Delivered
            ^                  |
            |  retryable fail  |  permanent fail or attempts used up
            +--or lease expiry-+--------------------------> Dead
            ^                                                 |
            +------------------- manual requeue --------------+
```

## Quick start

Requires Go 1.27 or newer.

```sh
# 1. fake provider (accepts email, sms and webhook calls, can inject failures)
go run ./cmd/peal sink -addr :9000 -fail-rate 0.2

# 2. broker: HTTP API on :8080, worker RPC on :9090
go run ./cmd/peal broker -http :8080 -rpc :9090 -data-dir ./data -fsync always

# 3. one or more workers
go run ./cmd/peal worker -broker localhost:9090 -pool 16
```

Submit a notification:

```sh
curl -i -X POST localhost:8080/v1/notifications -d '{
  "idempotency_key": "order-1042-shipped",
  "subject": "Your order shipped",
  "body": "Order 1042 is on its way.",
  "targets": [
    {"channel": "email",   "address": "ada@example.com"},
    {"channel": "webhook", "address": "http://localhost:9000/hook"}
  ]
}'
```

Check on it:

```sh
curl localhost:8080/v1/notifications/<id>
```

## HTTP API

| Method and path | Purpose |
| --- | --- |
| `POST /v1/notifications` | Submit (202 new, 200 duplicate key, 409 key reused with a different payload) |
| `GET /v1/notifications/{id}` | Status, per-delivery state, attempt history |
| `GET /v1/dead-letters` | List dead deliveries |
| `POST /v1/deliveries/{id}/requeue` | Revive a dead delivery |
| `GET /healthz`, `GET /readyz` | Liveness and readiness |
| `GET /debug/vars` | Counters (expvar) |

Errors share one shape: `{"error": {"code": "queue_full", "message": "..."}}`.

## Guarantees and limits

- **At-least-once.** A recipient can occasionally receive a duplicate, for example after a worker freezes past its lease. A late `Complete` from that worker is rejected by its stale token.
- **No ordering guarantees** between notifications or between deliveries.
- **Durability.** With `fsync=always`, every acknowledged (202) notification survives `kill -9` and restart. With `fsync=interval`, up to 50 ms of acknowledgements may be lost.
- **Single broker.** The broker is a single point of failure by design; replication is out of scope.
- **Backpressure.** Above 50,000 non-terminal deliveries, submits get `429` with `Retry-After`.

## Project layout

```
peal/
  cmd/peal/            subcommands: broker | worker | sink | loadgen
  internal/
    model/             Notification, Delivery, enums, validation
    clock/             Clock interface, real and fake implementations
    state/             state machine: Apply(Record), queues, leases, invariants
    wal/               Log interface, NopLog, FileLog, snapshots
    broker/            ingest, lease handling, scheduler loop
    api/               public HTTP handlers
    rpcproto/          RPC types shared by broker and workers
    worker/            lease loop, heartbeat, runner pool
    channel/           Sender interface, console and HTTP adapters
    resilience/        backoff, token bucket, circuit breaker
    sink/              fake provider with fault injection
    obs/               slog setup, expvar counters
  test/e2e/            process-level and chaos tests
```

## Roadmap

- [ ] **M0** Skeleton: module, `model`, `clock`, test script
- [ ] **M1** State machine with invariants and fake-clock tests
- [ ] **M2** HTTP API: submit, status, idempotency, backpressure
- [ ] **M3** RPC layer and first worker (`console` adapter)
- [ ] **M4** Real channels, retries, dead letters, rate limits, circuit breaker
- [ ] **M5** Write-ahead log: group commit, recovery, snapshots
- [ ] **M6** Observability and a measured performance fix
- [ ] **M7** Stretch: fallback routing, priorities, cancel, templates

## Testing

```sh
go vet ./...
go test -race -count=20 ./...
go test -fuzz=FuzzDecodeRecord ./internal/wal
```

The failure drills (worker `kill -9`, worker `SIGSTOP`, broker crash, torn WAL write, slow and dead providers, queue flood) live in `test/e2e`.

## Why "Peal"?

A peal is a loud ringing of bells: one ring that carries to everyone within earshot.
