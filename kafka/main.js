import nodemailer from 'nodemailer';
import { loadEnvFile } from 'node:process';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

// Resolve .env beside this file, even when Node is started from another folder.
try {
    loadEnvFile(fileURLToPath(new URL('./.env', import.meta.url)));
} catch (error) {
    if (error.code !== 'ENOENT') throw error;
}

function createAuth() {
    // Choose ONE authentication method; do not mix password and OAuth fields.
    const method = (process.env.SMTP_AUTH_METHOD || 'password').trim().toLowerCase();
    if (!['password', 'oauth2'].includes(method)) {
        throw new Error('SMTP_AUTH_METHOD must be password or oauth2.');
    }
    const required = method === 'oauth2'
        ? ['SMTP_USERNAME', 'OAUTH_CLIENTID', 'OAUTH_CLIENT_SECRET', 'OAUTH_REFRESH_TOKEN']
        : ['SMTP_USERNAME', 'SMTP_PASS'];
    const missing = required.filter(key => !process.env[key]?.trim());
    if (missing.length) {
        throw new Error('Fill these variables in kafka/.env: ' + missing.join(', '));
    }
    if (method === 'oauth2') {
        return {
            type: 'OAuth2',
            user: process.env.SMTP_USERNAME.trim(),
            clientId: process.env.OAUTH_CLIENTID.trim(),
            clientSecret: process.env.OAUTH_CLIENT_SECRET.trim(),
            refreshToken: process.env.OAUTH_REFRESH_TOKEN.trim()
        };
    }
    return {
        user: process.env.SMTP_USERNAME.trim(),
        // Use a Google App Password, NOT your regular Google account password.
        pass: process.env.SMTP_PASS.replace(/\s/g, '')
    };
}

// Create lazily so importing this module does not verify SMTP or send mail.
let transporter;
function getTransporter() {
    if (!transporter) {
        transporter = nodemailer.createTransport({
            host: 'smtp.gmail.com',
            port: 465,
            secure: true,
            auth: createAuth(),
            connectionTimeout: 10000,
            greetingTimeout: 10000,
            socketTimeout: 15000
        });
    }
    return transporter;
}

export async function sendOrderSuccessEmail(event, { topic, partition, offset }) {
    const to = process.env.MAIL_TO?.trim();
    if (!to) throw new Error('MAIL_TO_MISSING');
    // Plain text avoids interpreting event fields as HTML. Credentials stay local.
    const info = await getTransporter().sendMail({
        from: process.env.SMTP_USERNAME.trim(),
        to,
        subject: 'Order processed successfully: ' + event.orderId,
        text: [
            'Your Kafka consumer successfully processed an order.',
            'Order ID: ' + event.orderId,
            'Event ID: ' + event.eventId,
            'Amount (cents): ' + event.amountCents,
            'Event time: ' + event.time,
            'Topic: ' + topic,
            'Partition: ' + partition,
            'Offset: ' + offset
        ].join('\n')
    });
    // SMTP acceptance is not a guarantee that the email reached the inbox.
    if (!info.accepted?.length || info.rejected?.length) {
        throw new Error('RECIPIENT_NOT_ACCEPTED');
    }
    return info;
}

export function closeMailer() {
    transporter?.close();
}

// Running main.js directly tests SMTP. Importing it from consumer.js does not.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        await getTransporter().verify();
        console.log('SMTP connection and authentication succeeded. No email was sent.');
    } catch (error) {
        if (!transporter) console.error(error.message);
        else console.error('SMTP verification failed (' + (error.code || 'UNKNOWN') +
            '). Check credentials, Google account permissions, and network access.');
        process.exitCode = 1;
    } finally {
        closeMailer();
    }
}
