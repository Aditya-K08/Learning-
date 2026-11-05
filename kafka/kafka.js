import { Kafka } from 'kafkajs';

export const kafka = new Kafka({
    clientId: 'orders-service',
    brokers: ['localhost:9092'],

    connectionTimeout: 10000,
    requestTimeout: 30000,

    retry: {
        initialRetryTime: 300,
        retries: 8
    }
});