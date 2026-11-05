import { kafka, TOPICS, logMessage, signalStop } from './kafka.js';

const consumer = kafka.consumer({ groupId: 'orders-audit-v1' });
await consumer.connect();
await consumer.subscribe({ topics: [TOPICS.DEMO], fromBeginning: true });

consumer.on(consumer.events.GROUP_JOIN, e => {
  console.log('Group assignment:', JSON.stringify(e.payload.memberAssignment));
});
consumer.on(consumer.events.CRASH, e => {
  console.error('Kafka consumer crashed:', e.payload.error);
});
signalStop(() => consumer.disconnect());

await consumer.run({
  partitionsConsumedConcurrently: 3,
  eachMessage: async payload => {
    logMessage(payload);
    // Return only when the work really is complete.
    // KafkaJS then resolves this offset for auto-commit.
  },
});
