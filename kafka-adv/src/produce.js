import { randomUUID } from 'node:crypto';
import { createProducer, TOPICS } from './kafka.js';

const producer = createProducer();
try {
  await producer.connect();
  const orderId = randomUUID();
  const event = {
    eventId: randomUUID(),
    type: 'OrderCreated',
    version: 1,
    occurredAt: new Date().toISOString(),
    orderId,
    customerId: 'customer-123',
    amountCents: 2599,
  };
  const metadata = await producer.send({
    topic: TOPICS.DEMO,
    acks: -1,
    messages: [{
      key: orderId,
      value: JSON.stringify(event),
      headers: { 'event-type': 'OrderCreated', 'schema-version': '1' },
    }],
  });
  console.log('Published event:', event);
  console.log('Kafka delivery metadata:', metadata);
} finally {
  await producer.disconnect();
}
