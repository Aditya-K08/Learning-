// LAB 3 / TODO 6: build the transactional outbox API.
// The order and outbox event MUST be in the SAME PostgreSQL transaction.
import express from 'express';
import { randomUUID } from 'node:crypto';
import { inTransaction, pool } from './db.js';
import { TOPICS } from './kafka.js';
import { makeOrderCreatedEvent } from './event.js';

const app = express();
app.use(express.json({ limit: '64kb' }));
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.post('/orders', async (req, res) => {
  // TODO 6:
  // A. Validate Idempotency-Key header, nonempty customerId, positive integer amountCents.
  // B. Use inTransaction(async db => { ... })
  // C. Insert orders row (id = randomUUID(), client_request_id = header).
  // D. If same Idempotency-Key exists, return SAME order for same request,
  //    return 409 when request body differs.
  // E. For a newly inserted order, build an event with makeOrderCreatedEvent.
  // F. Insert matching event in outbox table *inside that transaction*.
  // G. Return {orderId,status,eventPublication:'pending'}, HTTP 201 / 200.
  // DO NOT directly call producer.send() here.
  res.status(501).json({ error: 'TODO 6: implement POST /orders' });
});

app.get('/orders/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, customer_id, amount_cents, status FROM orders WHERE id = $1',
      [req.params.id]
    );
    if (result.rowCount === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (error) {
    console.error(error);
    res.status(503).json({ error: 'Database unavailable' });
  }
});

const server = app.listen(3000, () => console.log('API on http://localhost:3000'));
let closing = false;
async function shutdown() {
  if (closing) return;
  closing = true;
  await new Promise(resolve => server.close(resolve));
  await pool.end();
}
process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
