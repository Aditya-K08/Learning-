import { kafka, TOPICS } from './kafka.js';

const admin = kafka.admin();
try {
  await admin.connect();
  for (const topic of Object.values(TOPICS)) {
    const offsets = await admin.fetchTopicOffsets(topic);
    console.log(topic, offsets);
  }
} finally {
  await admin.disconnect();
}
