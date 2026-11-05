# Build Kafka Yourself: 4-Hour Practical Workshop

**Do not read completed-reference code before completing the tasks.** This repo is intentionally incomplete and its unit tests initially FAIL. That is the point. Infrastructure files are done; you implement the Kafka behavior. This is a local development lab, NOT a production deployment.

## Prerequisites

- Node.js 20+ (`node --version`)
- Docker with `docker compose`
- curl or Postman

## Environment setup (10 minutes)

```bash
# Run from this project directory
docker compose up -d
npm install
cp .env.example .env      # PowerShell: Copy-Item .env.example .env
npm run topics
npm run check             # Syntax check. Should PASS before you start.
npm test                  # Initially FAILS: tests are your target.
```

Local endpoints: Kafka localhost:9092, PostgreSQL localhost:5432, later API localhost:3000. If topics creation initially fails, wait for Kafka startup and retry. If an old local Kafka container already uses 9092, stop it first.

The SQL schema is applied only when the postgres data volume is first initialized. `docker compose down -v` **deletes local database data**. `docker compose down` preserves it.

## HOUR 1 (0–60 min): Prove basic event delivery

**0–15 min | Tasks 1 and 2**: Edit `src/event.js` → implement `makeOrderCreatedEvent()`. Run `npm test`: at least the first two event tests should pass. Edit `src/produce.js`: connect once, send an event to `TOPICS.DEMO`, use `orderId` as record key, JSON stringify the value and include headers. Print response metadata. Run `npm run produce`. Verify `npm run inspect`.

**15–35 min | Task 3**: Edit `src/consume.js`: implement `connect`, `subscribe`, `run` and log topic/partition/offset/event data. In terminal A run `npm run consume`; in B run `npm run produce` multiple times. Leave the consumer running until Ctrl+C.

**35–50 min | Group experiment**: Open 3 terminals each running `npm run consume` (same default `groupId`). Observe `GROUP_JOIN` assignments. Then use `npm run consume -- orders-analytics-v1` in another terminal. Why does that group read the same retained messages independently? Note: `fromBeginning` does not reset an existing group's committed offsets.

**50–60 min | Key test**: Produce 10 messages using the *same* orderId: `npm run produce -- 11111111-1111-4111-8111-111111111111`. Observe partitions and offsets. Then use random IDs. Explain what guarantee the key gives and what it does NOT give.

**Checkpoint**: Can you answer what the partition and offset refer to, and how consumer-group IDs affect work distribution?

## HOUR 2 (60–120 min): Delivery, failure, idempotency

**60–75 min | Task 4**: Implement `parseOrderCreated()` in `src/event.js`. Validate all fields, including positive integer `amountCents`, UUIDs, type and version. Run `npm test` until the three event tests pass.

**75–100 min | Task 5/8 preparation**: Study `sql/schema.sql`, especially `processed_messages(consumer_name,event_id)` primary key. Edit `src/order-worker.js` to implement parsing and safe database-side idempotent processing using `inTransaction()`. The worker reads `TOPICS.ORDER_CREATED`, NOT `TOPICS.DEMO`. You will generate these messages via the outbox API in Hour 3.

**100–120 min | Failure analysis**: Write down the exact behavior for:
1. Crash before DB commit.
2. Crash after DB commit but before Kafka offset commit.
3. Duplicate event with same eventId.
4. A different eventId for the same order.
Your transaction must update the order and insert an inbox row atomically. The fourth case should NOT silently record a second state transition.

## HOUR 3 (120–180 min): Transactional outbox

**120–145 min | Task 6**: Implement `POST /orders` in `src/api.js`. Validate input. Make the `orders` and `outbox` inserts in the SAME PostgreSQL transaction. Support an `Idempotency-Key` HTTP header. Start `npm run api` and test:

```bash
curl -X POST http://localhost:3000/orders -H 'Content-Type: application/json' -H 'Idempotency-Key: order-demo-1' -d '{"customerId":"customer-123","amountCents":2599}'
```

PowerShell users: use `curl.exe` or Postman. Run the same request twice: the second should return the same order ID. Change the amount using the same key: it should return 409.

**145–165 min | Task 7**: Implement `src/outbox-relay.js`. It polls rows with `published_at IS NULL`, uses `FOR UPDATE SKIP LOCKED`, sends events with Kafka key `aggregate_id`, and marks published only after broker acknowledgment. Launch `npm run relay`.

**165–180 min | Integrate**: Launch `npm run worker` in another terminal. Submit an order. GET `/orders/<id>` and see the status change to `PAYMENT_RECORDED`. Inspect DB:

```bash
docker compose exec postgres psql -U app -d orders -c 'SELECT id,status FROM orders;'
docker compose exec postgres psql -U app -d orders -c 'SELECT event_type,published_at FROM outbox ORDER BY created_at;'
docker compose exec postgres psql -U app -d orders -c 'SELECT consumer_name,event_id FROM processed_messages;'
```

## HOUR 4 (180–240 min): Recover from failures

**180–195 min | Task 9**: Add DLQ handling for *permanent schema failures* to `src/order-worker.js`; do not DLQ transient database failures. Start `npm run dlq` in another terminal. Invalid messages must be published successfully to the DLQ before the original record can be acknowledged.

**195–210 min | Task 10**: Implement deterministic exponential backoff in `src/retry.js`; run `npm test` until all tests pass. Add bounded retry with jitter around a transient operation, but don't retry invalid payloads.

**210–225 min | Failure drills**:
- Stop Kafka: `docker compose stop kafka`. Create an order. Restart it: `docker compose start kafka`. Confirm outbox eventually flushes.
- Stop worker before creating an order. Start it later and confirm it catches up.
- Reproduce duplicate publication by restarting relay after a successful Kafka send but before an outbox DB commit (use a temporary manual fault injection before COMMIT); confirm inbox prevents duplicate business effects.

**225–240 min | Production review**: Be able to explain ordering limitations, retries, retention, `acks=-1`, replication factor, `min.insync.replicas`, schema compatibility, lag, shutdown, and why `Kafka transaction != external payment transaction`.

## Useful commands

```bash
npm run check     # Syntax only (should always pass)
npm test          # Pure unit tests (initially fail until your implementation)
npm run topics    # Create five topics
npm run inspect   # Fetch metadata for demo topic
npm run produce   # Demo event producer
npm run consume   # Demo event consumer (or append -- group-id)
npm run api       # HTTP order API
npm run relay     # Database outbox publisher
npm run worker    # Order processor with inbox dedupe
npm run dlq       # Dead-letter topic inspector
npm run lag       # Inspect orders-payment-worker-v1 offsets
```

## How to learn efficiently

Implement ONE TODO at a time; run its test or experiment; write down what happened, not just that it succeeded. Never skip the crash-window explanation. If stuck, read `HINTS.md` one hint at a time, then retry. The completed-reference project is available separately as a last resort; do not copy-paste it before attempting the exercise.

### Operational caution

This is a deliberately local, single-broker, no-auth, replication-factor-1 environment. It is not suitable for live workloads. A real system requires several brokers, security, schema governance, monitoring, rollout plans, and carefully tested recovery procedures.
