# Kafka — Build It Yourself (Node.js)

This is the **hands-on exercise starter**. Unlike the completed reference project, it deliberately contains TODOs in producer, consumer, outbox, and idempotent worker files. Your job is to implement them. The Docker/SQL setup and tools are supplied.

**Begin here: [LAB_GUIDE.md](LAB_GUIDE.md)**. If you get stuck, open [HINTS.md](HINTS.md) for a small clue, then try again.

## Start

```bash
docker compose up -d
npm install
cp .env.example .env # PowerShell: Copy-Item .env.example .env
npm run topics
npm run check
npm test # Expected to FAIL initially; implement TODO 1 then retry
```

Run `npm run produce` after implementing TODO 1 and TODO 2; run `npm run consume` after TODO 3. See the full hour-by-hour plan in LAB_GUIDE.md.

**Important:** This is a local education lab, not a production template. Kafka is single broker/replication factor 1 and is intentionally unauthenticated. No real money is charged; `PAYMENT_RECORDED` is a simulated status transition.
