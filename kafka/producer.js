import { version } from 'node:os';
import { kafka } from './kafka.js';
import { randomUUID } from 'node:crypto';

const producer = kafka.producer({
    allowAutoTopicCreation: false
});

await producer.connect();

const orderId = randomUUID();
const event = {
    eventId : randomUUID(),
    type : 'order created',
    orderId,
    version : 1,
    amountCents : 2500,
    time : new Date().toISOString()
}
const secondOrderId = randomUUID();
const secondEvent = {
    ...event,
    eventId: randomUUID(),
    orderId: secondOrderId
};
try{

    await producer.send({
        topic : 'order.created.v1',
        acks : -1,
        messages : [
            {
                key : orderId,
                value : JSON.stringify(event),
                headers: {
                    'event-type': 'OrderCreated',
                    'schema-version': '1'
                }
            },
            {
                key : randomUUID(),
                value : JSON.stringify(secondEvent),
                headers: {
                    'event-type': 'OrderCreated',
                    'schema-version': '1'
                }
            }
        ]
    })
    console.log('Kafka acknowledged the event');
}finally{
    await producer.disconnect();
}