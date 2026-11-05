# Progressive hints (only read the task you are stuck on)

**Task 1 event**: Use `randomUUID()` for `eventId`, `new Date().toISOString()` for `occurredAt`. Validate `Number.isSafeInteger(amountCents) && amountCents > 0` and non-empty strings.

**Task 2 produce**: `await producer.send({topic: TOPICS.DEMO, acks:-1, messages:[{key:orderId,value:JSON.stringify(event),headers:{...}}]})`. A `key` is not a unique message ID.

**Task 3 consume**: Create the client with `kafka.consumer({groupId})`. `await consumer.subscribe({topics:[TOPICS.DEMO],fromBeginning:true})`. `eachMessage` receives `{topic,partition,message}`. `message.value` is a Buffer.

**Task 4 parse**: `JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf8') : raw)`. Validate UUIDs and event envelope; reject unexpected versions.

**Task 5 / 8 inbox**: `INSERT ... ON CONFLICT DO NOTHING` returns `rowCount===0` for an already-processed event. Keep the inbox insert and `UPDATE orders` inside `inTransaction`. Do not acknowledge after a failed DB transaction.

**Task 6 outbox API**: Keep `INSERT orders` and `INSERT outbox` in one `inTransaction`. `client_request_id` is unique. For repeated request IDs, read existing order and compare customer/amount. Direct Kafka publication inside the HTTP handler recreates dual-write risk.

**Task 7 relay**: Select `WHERE published_at IS NULL ORDER BY created_at,id LIMIT 20 FOR UPDATE SKIP LOCKED`. Send each record with key `row.aggregate_id`, then `UPDATE outbox SET published_at=now()`. This single-instance lab still allows at-least-once duplicates after crashes.

**Task 9 DLQ**: Catch only parse/schema exceptions. Publish original `Buffer` value and Kafka key to `TOPICS.DLQ` with origin headers, await the DLQ `producer.send`, then return. Let database errors propagate.

**Task 10 backoff**: `Math.min(capMs, baseMs * 2 ** (attempt - 1))`. For production retry loops, cap attempts, classify transient errors, and add jitter.
