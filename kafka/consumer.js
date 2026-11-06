import { kafka } from "./kafka.js";
import { sendOrderSuccessEmail, closeMailer } from "./main.js";

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

        // Send one email after each order is processed, not once per producer run.
        // Await SMTP so we can report whether the server accepted this email.
        try {
            await sendOrderSuccessEmail(event, { topic, partition, offset: message.offset });
            console.log('SMTP accepted the notification for', event.orderId);
        } catch (error) {
            // Keep consuming orders if email fails. This demo does not retry failed
            // emails; returning lets Kafka commit this record's offset normally.
            const reason = error.code || (error.message === 'MAIL_TO_MISSING'
                ? 'Set MAIL_TO in kafka/.env'
                : 'Check SMTP credentials and recipient configuration');
            console.error('Email failed for order', event.orderId, '-', reason);
        }
    }
})
// Disconnect cleanly when you press Ctrl+C.
process.once('SIGINT', async () => {
    await consumer.disconnect();
    closeMailer();
});
