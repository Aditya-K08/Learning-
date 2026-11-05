// LAB 2-4 / TODO 5, 8, 9: idempotent consumer + DLQ.
import { randomUUID } from 'node:crypto';
import { kafka, createProducer, TOPICS } from './kafka.js';
import { inTransaction, pool } from './db.js';
import { parseOrderCreated } from './event.js';

const groupId = 'orders-payment-worker-v1';
const consumer = kafka.consumer({ groupId, sessionTimeout: 30_000 });
const dlqProducer = createProducer();

await consumer.connect();
await dlqProducer.connect();
await consumer.subscribe({ topics: [TOPICS.ORDER_CREATED], fromBeginning: true });

// TODO 9 (Hour 4): use this for permanent invalid-event failures ONLY.
async function publishToDLQ(payload, error) {
  // Keep original key and value (even malformed JSON), plus headers:
  // original-topic, original-partition, original-offset, failure-reason.
  // Wait for successful publication BEFORE returning from handler.
  throw new Error('TODO 9: implement DLQ publication');
}

let closing = false;
async function shutdown() {
  if (closing) return;
  closing = true;
  await consumer.disconnect();
  await dlqProducer.disconnect();
  await pool.end();
}
process.once('SIGINT', () => void shutdown().catch(console.error));
process.once('SIGTERM', () => void shutdown().catch(console.error));

await consumer.run({
  partitionsConsumedConcurrently: 3,
  eachMessage: async payload => {
    // TODO 5: try parseOrderCreated(payload.message.value).
    // If invalid, in LAB 4 call publishToDLQ(payload,error) and return;
    // until then throw, so the bad record is NOT acknowledged.

    // TODO 8: use inTransaction(async db => ...)
    // A. INSERT processed_messages(consumer_name,event_id)
    //       ON CONFLICT DO NOTHING.
    // B. If duplicate, return without changing order state.
    // C. UPDATE orders SET status='PAYMENT_RECORDED'
    //    WHERE id=$1 AND status='CREATED'; assert 1 affected row.
    // D. INSERT an OrderPaid event into outbox in the same DB transaction.
    // E. Return successfully ONLY after the transaction commits.
    // No direct charge is performed; payment state is simulated.
    throw new Error('TODO 5/8: implement idempotent event handler');
  }
});
