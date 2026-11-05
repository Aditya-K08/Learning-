import express from 'express';
import { randomUUID } from 'node:crypto';
import { inTransaction, pool } from './db.js';
import { TOPICS } from './kafka.js';

const app = express();
app.use(express.json({ limit: '64kb' }));

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.post('/orders', async (req, res) => {
  const requestId = req.get('Idempotency-Key');
  const { customerId, amountCents } = req.body ?? {};
  if (typeof requestId !== 'string' || !requestId.trim() || requestId.length > 150 ||
      typeof customerId !== 'string' || !customerId.trim() || customerId.length > 200 ||
      !Number.isSafeInteger(amountCents) || amountCents <= 0) {
    return res.status(400).json({
      error: 'Provide Idempotency-Key, customerId, and positive integer amountCents',
    });
  }

  try {
    const result = await inTransaction(async db => {
      const orderId = randomUUID();
      const insertion = await db.query(
        `INSERT INTO orders(id, client_request_id, customer_id, amount_cents, status)
         VALUES ($1, $2, $3, $4, 'CREATED')
         ON CONFLICT (client_request_id) DO NOTHING
         RETURNING id, client_request_id, customer_id, amount_cents, status`,
        [orderId, requestId, customerId, amountCents]
      );

      if (insertion.rowCount === 0) {
        const existing = await db.query(
          'SELECT id, client_request_id, customer_id, amount_cents, status FROM orders WHERE client_request_id = $1',
          [requestId]
        );
        if (!existing.rows.length) throw new Error('Conflicting request not visible');
        const order = existing.rows[0];
        if (order.customer_id !== customerId || Number(order.amount_cents) !== amountCents) {
          return { conflict: true };
        }
        return { created: false, order };
      }

      const event = {
        eventId: randomUUID(),
        type: 'OrderCreated',
        version: 1,
        occurredAt: new Date().toISOString(),
        orderId,
        customerId,
        amountCents,
      };
      await db.query(
        `INSERT INTO outbox (id, topic, aggregate_id, event_type, payload)
         VALUES ($1, $2, $3, $4, $5::jsonb)`,
        [event.eventId, TOPICS.ORDER_CREATED, orderId, event.type, JSON.stringify(event)]
      );
      return { created: true, order: insertion.rows[0] };
    });

    if (result.conflict) return res.status(409).json({ error: 'Idempotency key reused with different request data' });
    res.status(result.created ? 201 : 200).json({
      orderId: result.order.id,
      status: result.order.status,
      eventPublication: 'pending', // Outbox relay publishes asynchronously.
    });
  } catch (error) {
    console.error('Create order failed', error);
    res.status(503).json({ error: 'Service temporarily unavailable' });
  }
});

app.get('/orders/:id', async (req, res) => {
  try {
    const rows = await pool.query('SELECT id, customer_id, amount_cents, status FROM orders WHERE id = $1', [req.params.id]);
    if (!rows.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(rows.rows[0]);
  } catch (error) {
    console.error(error);
    res.status(503).json({ error: 'Service temporarily unavailable' });
  }
});

const server = app.listen(Number(process.env.PORT || 3000), () => {
  console.log(`HTTP API listening on http://localhost:${process.env.PORT || 3000}`);
});

async function stop() {
  await new Promise((resolve, reject) => server.close(err => err ? reject(err) : resolve()));
  await pool.end();
}
process.once('SIGINT', () => void stop().catch(console.error));
process.once('SIGTERM', () => void stop().catch(console.error));
