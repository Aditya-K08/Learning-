// LAB 1 / TODO 2: first producer you implement yourself.
import { randomUUID } from 'node:crypto';
import { createProducer, TOPICS } from './kafka.js';
import { makeOrderCreatedEvent } from './event.js';

const orderId = process.argv[2] || randomUUID();
const event = makeOrderCreatedEvent({
  orderId,
  customerId: 'customer-123',
  amountCents: 2599,
});

const producer = createProducer();
try {
  await producer.connect();
  // TODO 2: producer.send({topic: TOPICS.DEMO, acks: -1, messages:[...]})
  // Key MUST be orderId; value MUST be JSON.stringify(event).
  // Add headers event-type, event-id and schema-version.
  // Log returned Kafka metadata: topic, partition and baseOffset.
  // Do not treat a thrown send as success.
  throw new Error('TODO 2: publish the event using producer.send');
} finally {
  await producer.disconnect();
}
