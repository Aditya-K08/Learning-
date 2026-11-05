import { randomUUID } from 'node:crypto';

// LAB 1 / TODO 1
// Return a valid, NEW OrderCreated event. UUIDs: randomUUID().
// Fields required: eventId, type, version, occurredAt,
// orderId, customerId, amountCents.
// Validation: amountCents is a positive safe integer.
export function makeOrderCreatedEvent({ orderId, customerId, amountCents }) {
  throw new Error('TODO 1: implement makeOrderCreatedEvent');
}

// LAB 2 / TODO 4
// Accept Buffer or string; JSON.parse it; validate required fields;
// reject wrong type, version, bad UUIDs, or invalid amount.
// Throw for invalid data. Never silently return null.
export function parseOrderCreated(raw) {
  throw new Error('TODO 4: implement parseOrderCreated');
}
