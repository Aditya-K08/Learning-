import { kafka, TOPICS, logMessage, signalStop } from './kafka.js';

const consumer = kafka.consumer({ groupId: 'orders-dlq-inspector-v1' });
await consumer.connect();
await consumer.subscribe({ topics: [TOPICS.DLQ], fromBeginning: true });
signalStop(() => consumer.disconnect());
await consumer.run({ eachMessage: async payload => logMessage(payload) });
