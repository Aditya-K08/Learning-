import { kafka } from "./kafka.js";

const consumer = kafka.consumer({
    groupId : 'notification-service-v1'
})

await consumer.connect();

await consumer.subscribe({
    topics: ['order.created.v1'],
    fromBeginning : true
})

await consumer.run({
    eachMessage : async ({topic , partition , message}) =>{
        const event = JSON.parse(
            message.value.toString('utf8')
        )
        console.log({
            topic, 
            partition,
            offset : message.offset,
            orderId : event.orderId
        })
        console.log('Notification requested for', event.orderId);
    }
})