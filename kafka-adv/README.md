# Kafka: Four-Hour JavaScript Bootcamp

An executable learning lab for Apache Kafka + KafkaJS + PostgreSQL.

## Requirements

- Docker and Docker Compose
- Node.js 20+ and npm
- `curl` (or Postman)

**Important:** The Docker Compose cluster is a **single-broker, no-TLS, replication-factor-1 development setup only**. Never use it for real production traffic. This repo intentionally demonstrates exactly-once *business effects through database transactions + idempotency*, **not** exactly-once Kafka deliveries.

## 1. Start your local services

```bash
docker compose up -d
npm install
cp .env.example .env     # On Windows PowerShell: Copy-Item .env.example .env
npm run topics
```

Kafka broker: `localhost:9092`. PostgreSQL: `localhost:5432`. Creating topics immediately after Docker startup can fail while the broker is still starting; repeat `npm run topics` a few seconds later. Check `docker compose logs -f kafka` if it does not start.

The PostgreSQL SQL schema is applied automatically only when the `pgdata` volume is first initialized. To reset local development **and erase all local PostgreSQL data**, run `docker compose down -v` then `docker compose up -d`.

## 2. First Kafka experiment: producer + consumer

Terminal A:

```bash
npm run consume
```

Terminal B:

```bash
npm run produce
npm run inspect

# After the database-backed worker starts, inspect its consumer-group lag:
# npm run lag
```

Run producer repeatedly. Add two more consumer terminals with `npm run consume`; they share the group `orders-audit-v1`, and between them will split three partitions. A different consumer group gets the same complete stream, not a share of the original group's records.

The simple demo uses `demo.orders.created.v1`, separate from the database-backed pipeline topic `orders.created.v1`, so the production worker will not see standalone demo messages.

The `fromBeginning: true` option **does not reset an existing group's committed positions**. Use a new group ID or an explicit offset reset while the group is stopped to replay.

## 3. Outbox + inbox production patterns

The design:

```text
POST /orders
    | single PostgreSQL transaction
    +--> orders row
    +--> outbox row
               |
         outbox-relay
               |
      orders.created.v1
               |
          order-worker
               | single PostgreSQL transaction
               +--> processed_messages row (idempotency inbox)
               +--> status update (simulated, no actual payment)
               +--> new outbox row
                                |
                          outbox-relay
                                |
                         orders.paid.v1
```

Open four terminals:

```bash
# A: HTTP API
npm run api

# B: relay (just one process for this learning project)
npm run relay

# C: idempotent order-processing worker
npm run worker

# D: optional dead-letter queue watcher
npm run dlq
```

Create a sample order:

```bash
curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: customer-123-order-001' \
  -d '{"customerId":"customer-123","amountCents":2599}'
```

The response contains `orderId`. Run again with the **same** idempotency key and same body: the API returns the same order ID without inserting a duplicate order/outbox event. Reuse the key with a different amount: HTTP 409.

Check status:

```bash
curl http://localhost:3000/orders/<orderId>
```

After relay + worker processing it becomes `PAYMENT_RECORDED`. **This merely simulates the state transition; it does not charge a card or integrate with a payment gateway.** For real payments, pass a durable payment-provider idempotency key and design recovery for ambiguous provider timeouts.

## 4. Failure drills (do these!)

### Drill 1: Broker downtime

Keep your API and worker/relay terminals running, then:

```bash
docker compose stop kafka
```

POST another order. Because PostgreSQL is running, the order and its outbox event are committed, even though Kafka is unavailable. Restore Kafka:

```bash
docker compose start kafka
```

The relay should eventually publish. If the relay process stopped, restart it with `npm run relay`. This is why the outbox exists.

### Drill 2: Worker downtime

Stop the worker, create two orders, then restart the worker. It should catch up from its committed Kafka offsets.

### Drill 3: Duplicate event

The basic producer writes to a different topic. To test real duplicate delivery, replay an existing valid Kafka message from `orders.created.v1` *with its original `eventId`*. The inbox unique key makes the database business effect happen only once.

### Drill 4: Permanent invalid event + DLQ

Produce an invalid JSON record to `orders.created.v1` (for example with the Kafka console producer or a modified `src/produce.js`). The worker sends the malformed record to `orders.created.v1.dlq` and returns successfully **only after the DLQ publish succeeds**. Watch `npm run dlq`.

### Drill 5: Relay duplicates

If the relay publishes to Kafka and crashes before PostgreSQL commits `published_at`, it will publish the event again after restart. This cannot be completely eliminated with this cross-system design; the worker handles the duplicate using `processed_messages`.

## Important caveats before real production

1. This is a teaching project, **not a completed production deployment**. It lacks authentication, validation via a schema registry, distributed tracing, formal health/readiness probes, metrics, load tests, schema migrations, secrets management, controlled rollouts, and robust supervisord/orchestrator restart policy.
2. The relay is intentionally **single-active-instance** to make the ordering story easier. `FOR UPDATE SKIP LOCKED` is included to demonstrate row leasing, but deploying multiple relay workers can reorder events for a given aggregate; strict aggregate ordering requires an explicit design (e.g., aggregate versioning + per-aggregate publication ordering or CDC).
3. The worker throws on unexpected database state to avoid silently acknowledging a broken invariant. You must add bounded retries and operational escalation; an always-failing record will block a partition.
4. The DLQ is used **only for permanent invalid-data failures** in this example. Infrastructure failures are retried. Never silently classify a database outage as a malformed message.
5. Kafka topic retention can delete messages that an offline consumer never read. Size retention for your maximum supported outage and recovery requirements.
6. `acks: -1` does not magically create redundancy when replication factor is 1. For production use multiple brokers, replication factor 3, `min.insync.replicas` 2, monitored ISR, and tested failover.
7. Keep Kafka messages small; put files/videos in object storage and publish object metadata and references.

## Inspect PostgreSQL data

```bash
docker compose exec postgres psql -U app -d orders \
  -c "SELECT id,status FROM orders;"

docker compose exec postgres psql -U app -d orders \
  -c "SELECT event_type,published_at FROM outbox ORDER BY created_at;"

docker compose exec postgres psql -U app -d orders \
  -c "SELECT consumer_name,event_id FROM processed_messages;"
```

## Helpful references

- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/)
- [KafkaJS producer](https://kafka.js.org/docs/producing)
- [KafkaJS consumer](https://kafka.js.org/docs/consuming)
- [KafkaJS transactions](https://kafka.js.org/docs/transactions)
- [Confluent JavaScript client migration](https://docs.confluent.io/kafka-clients/javascript/current/migration.html)
- [Debezium outbox](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
