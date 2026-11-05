import { randomUUID } from 'node:crypto';
import { kafka, TOPICS, asJson } from './kafka.js';
import { inTransaction, pool } from './db.js';
import { createProducer } from './kafka.js';

const consumer = kafka.consumer({ groupId: 'orders-payment-worker-v1', sessionTimeout: 30_000 });
const dlqProducer = createProducer();

function validateEvent(event) {
  return event?.type === 'OrderCreated' &&
    event?.version === 1 &&
    typeof event?.eventId === 'string' &&
    /^[0-9a-f-]{36}$/i.test(event.eventId) &&
    typeof event?.orderId === 'string' &&
    /^[0-9a-f-]{36}$/i.test(event.orderId) &&
    Number.isSafeInteger(event.amountCents) && event.amountCents > 0;
}

async function publishDLQ({ topic, partition, message }, error) {
  await dlqProducer.send({
    topic: TOPICS.DLQ,
    acks: -1,
    messages: [{
      key: message.key ?? null,
      value: message.value ?? Buffer.from(''),
      headers: {
        'failure-reason': String(error.message).slice(0, 500),
        'source-topic': topic,
        'source-partition': String(partition),
        'source-offset': message.offset,
      },
    }],
  });
  console.warn('Moved permanent invalid record to DLQ', { topic, partition, offset: message.offset });
}

await dlqProducer.connect();
await consumer.connect();
await consumer.subscribe({ topics: [TOPICS.ORDER_CREATED], fromBeginning: true });

let closing = false;
async function stop() {
  if (closing) return;
  closing = true;
  await consumer.disconnect();
  await dlqProducer.disconnect();
  await pool.end();
}
process.once('SIGINT', () => void stop().catch(console.error));
process.once('SIGTERM', () => void stop().catch(console.error));
consumer.on(consumer.events.CRASH, e => console.error('Consumer crashed', e.payload.error));

await consumer.run({
  partitionsConsumedConcurrently: 3,
  eachMessage: async payload => {
    let event;
    try {
      event = asJson(payload.message);
      if (!validateEvent(event)) throw new Error('Invalid OrderCreated schema');
    } catch (error) {
      // Only permanent data/schema errors go to DLQ here.
      // If DLQ publication fails, THROW so the source offset is not resolved.
      await publishDLQ(payload, error);
      return;
    }

    // Simulated payment state transition only. NOT a real payment processor.
    // Deduplication, DB state update, and new outbox event are atomic.
    await inTransaction(async db => {
      const inserted = await db.query(
        `INSERT INTO processed_messages (consumer_name, event_id)
         VALUES ($1, $2) ON CONFLICT DO NOTHING`,
        ['orders-payment-worker-v1', event.eventId]
      );
      if (!inserted.rowCount) {
        console.log('Duplicate event skipped:', event.eventId);
        return;
      }

      const result = await db.query(
        `UPDATE orders SET status = 'PAYMENT_RECORDED'
         WHERE id = $1 AND status = 'CREATED'
         RETURNING id`,
        [event.orderId]
      );
      if (!result.rowCount) {
        // Don't silently acknowledge state inconsistencies.
        throw new Error(`Order ${event.orderId} missing or in unexpected state`);
      }

      const eventId = randomUUID();
      const nextEvent = {
        eventId,
        type: 'OrderPaid',
        version: 1,
        occurredAt: new Date().toISOString(),
        orderId: event.orderId,
      };
      await db.query(
        `INSERT INTO outbox(id, topic, aggregate_id, event_type, payload)
         VALUES ($1, $2, $3, $4, $5::jsonb)`,
        [eventId, TOPICS.ORDER_PAID, event.orderId, nextEvent.type, JSON.stringify(nextEvent)]
      );
    });
    console.log('Processed order event:', event.eventId);
  },
});
