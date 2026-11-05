import { pool, inTransaction } from './db.js';
import { createProducer } from './kafka.js';

const producer = createProducer();
let running = true;
process.once('SIGINT', () => { running = false; });
process.once('SIGTERM', () => { running = false; });
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

await producer.connect();
console.log('Outbox relay running. Run only one instance of this simple demo relay.');
try {
  while (running) {
    try {
      const count = await inTransaction(async db => {
        const { rows } = await db.query(
          `SELECT id, topic, aggregate_id, event_type, payload
           FROM outbox WHERE published_at IS NULL
           ORDER BY created_at, id
           LIMIT 20 FOR UPDATE SKIP LOCKED`
        );
        for (const row of rows) {
          await producer.send({
            topic: row.topic,
            acks: -1,
            messages: [{
              key: row.aggregate_id,
              value: JSON.stringify(row.payload),
              headers: { 'event-type': row.event_type, 'event-id': row.id },
            }],
          });
          await db.query('UPDATE outbox SET published_at = now() WHERE id = $1', [row.id]);
        }
        return rows.length;
      });
      if (count) console.log(`Published ${count} outbox event(s)`);
      if (!count) await sleep(500);
    } catch (error) {
      // On a failure the transaction rolls back; successfully sent Kafka events
      // may therefore be published again. The consumer's inbox handles that.
      console.error('Relay error; will retry', error);
      await sleep(1_000);
    }
  }
} finally {
  await producer.disconnect();
  await pool.end();
}
