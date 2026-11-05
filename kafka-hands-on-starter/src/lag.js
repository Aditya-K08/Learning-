import { kafka, TOPICS } from './kafka.js';

const groupId = process.argv[2] || 'orders-payment-worker-v1';
const topic = process.argv[3] || TOPICS.ORDER_CREATED;
const admin = kafka.admin();
try {
  await admin.connect();
  const [ends, groups] = await Promise.all([
    admin.fetchTopicOffsets(topic),
    admin.fetchOffsets({ groupId, topics: [topic] }),
  ]);
  const groupPartitions = groups.find(g => g.topic === topic)?.partitions || [];
  const committedByPartition = new Map(groupPartitions.map(p => [p.partition, p.offset]));
  for (const end of ends) {
    const committed = committedByPartition.get(end.partition);
    const lag = committed == null || committed === '-1'
      ? 'unavailable (no committed position)'
      : (BigInt(end.offset) - BigInt(committed)).toString();
    console.log({ groupId, topic, partition: end.partition, endOffset: end.offset, committed, lag });
  }
} finally {
  await admin.disconnect();
}
