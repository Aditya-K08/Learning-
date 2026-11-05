import { kafka, TOPICS } from './kafka.js';

const admin = kafka.admin();
try {
  await admin.connect();
  const created = await admin.createTopics({
    waitForLeaders: true,
    topics: Object.values(TOPICS).map(topic => ({
      topic,
      numPartitions: 3,
      replicationFactor: 1, // Local one-broker development only!
      configEntries: [
        { name: 'cleanup.policy', value: 'delete' },
        { name: 'retention.ms', value: '604800000' },
      ],
    })),
  });
  console.log(created ? 'Topics created' : 'Topics already existed');
  const meta = await admin.fetchTopicMetadata({ topics: Object.values(TOPICS) });
  console.dir(meta.topics.map(t => ({
    name: t.name,
    partitions: t.partitions.map(p => p.partitionId),
  })), { depth: 5 });
} finally {
  await admin.disconnect();
}
