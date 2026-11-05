// LAB 3 / TODO 7: relay committed outbox rows into Kafka.
import { createProducer } from './kafka.js';
import { inTransaction, pool } from './db.js';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const producer = createProducer();
let running = true;
process.once('SIGINT', () => { running = false; });
process.once('SIGTERM', () => { running = false; });

await producer.connect();
try {
  while (running) {
    try {
      // TODO 7: inTransaction(async db => {
      //  SELECT unpublished outbox rows ORDER BY created_at, id
      //  LIMIT 20 FOR UPDATE SKIP LOCKED
      //  For each row, await producer.send() using aggregate_id as key;
      //  ONLY AFTER an acknowledged send, UPDATE published_at = now().
      // })
      // NOTE: after Kafka succeeds, DB commit can still fail => duplicate
      // Kafka publication on restart. Our consumer must dedupe.
      throw new Error('TODO 7: implement outbox relay batch');
    } catch (error) {
      console.error('Relay failure (retrying):', error.message);
      await sleep(1000);
    }
  }
} finally {
  await producer.disconnect();
  await pool.end();
}
