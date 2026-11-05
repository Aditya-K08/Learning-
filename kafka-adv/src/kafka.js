import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';

export const TOPICS = Object.freeze({
  DEMO: 'demo.orders.created.v1',
  ORDER_CREATED: 'orders.created.v1',
  ORDER_PAID: 'orders.paid.v1',
  DLQ: 'orders.created.v1.dlq',
});

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'orders-bootcamp',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  connectionTimeout: 10_000,
  requestTimeout: 30_000,
  retry: { initialRetryTime: 300, retries: 8 },
  logLevel: logLevel.WARN,
});

export function createProducer() {
  return kafka.producer({ allowAutoTopicCreation: false });
}

export function asJson(message) {
  if (!message.value) throw new Error('Missing message value');
  return JSON.parse(message.value.toString('utf8'));
}

export function logMessage({ topic, partition, message }) {
  console.log(JSON.stringify({
    topic,
    partition,
    offset: message.offset,
    key: message.key?.toString('utf8') ?? null,
    value: message.value?.toString('utf8') ?? null,
  }));
}

export function signalStop(stop) {
  let stopping = false;
  const handler = () => {
    if (stopping) return;
    stopping = true;
    Promise.resolve(stop()).catch(err => {
      console.error('Shutdown error', err);
      process.exitCode = 1;
    });
  };
  process.once('SIGINT', handler);
  process.once('SIGTERM', handler);
}
