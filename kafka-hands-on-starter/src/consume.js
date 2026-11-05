// LAB 1 / TODO 3: consumer + consumer groups + ordering.
import { kafka, TOPICS, logMessage, signalStop } from './kafka.js';

// Pass a custom group on CLI, e.g. npm run consume -- audit-2
const groupId = process.argv[2] || 'orders-audit-v1';
const consumer = kafka.consumer({ groupId });

// TODO 3:
// 1. connect()
// 2. subscribe({topics:[TOPICS.DEMO], fromBeginning:true})
// 3. register GROUP_JOIN event to print partition assignments
// 4. signalStop(() => consumer.disconnect())
// 5. run({ partitionsConsumedConcurrently: 3, eachMessage: async payload => {...} })
// Within eachMessage: JSON.parse(value) and log eventId, orderId, key,
// partition and offset, then logMessage(payload).
// Intentionally leave auto-commit enabled for this first exercise.
throw new Error('TODO 3: connect, subscribe, run your consumer');
