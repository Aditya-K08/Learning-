import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { makeOrderCreatedEvent, parseOrderCreated } from '../src/event.js';

test('creates a valid stable-contract event', () => {
  const orderId = randomUUID();
  const event = makeOrderCreatedEvent({ orderId, customerId: 'customer-1', amountCents: 2599 });
  assert.match(event.eventId, /^[0-9a-f-]{36}$/i);
  assert.equal(event.type, 'OrderCreated');
  assert.equal(event.version, 1);
  assert.equal(event.orderId, orderId);
  assert.equal(event.customerId, 'customer-1');
  assert.equal(event.amountCents, 2599);
  assert.ok(Number.isFinite(Date.parse(event.occurredAt)));
});

test('rejects invalid input', () => {
  assert.throws(() => makeOrderCreatedEvent({ orderId: randomUUID(), customerId: 'c', amountCents: -2 }));
  assert.throws(() => makeOrderCreatedEvent({ orderId: randomUUID(), customerId: '', amountCents: 100 }));
});

test('parses valid Buffer and rejects malformed events', () => {
  const event = makeOrderCreatedEvent({ orderId: randomUUID(), customerId: 'customer-1', amountCents: 100 });
  assert.deepEqual(parseOrderCreated(Buffer.from(JSON.stringify(event))), event);
  assert.throws(() => parseOrderCreated(Buffer.from('{invalid-json')));
  assert.throws(() => parseOrderCreated(Buffer.from(JSON.stringify({ ...event, type: 'Other' }))));
  assert.throws(() => parseOrderCreated(Buffer.from(JSON.stringify({ ...event, amountCents: '100' }))));
});
