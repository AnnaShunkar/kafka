# Kafka Dashboard (Learning Setup)

This is a recreated, runnable version inspired by
[uttesh/kafkaclient](https://github.com/uttesh/kafkaclient) for learning how Kafka works with a frontend and backend.

## What is included

- `client`: React + TypeScript + MUI dashboard
- `server`: Express + KafkaJS API
- `docker-compose.yml`: single-node Kafka (no Zookeeper)

## Run it

1. Start Kafka:

```bash
docker compose up -d
```

2. Start backend:

```bash
cd server
npm install
npm run dev
```

3. Start frontend:

```bash
cd ../client
npm install
npm run dev
```

4. Open the Vite URL shown in terminal (usually `http://localhost:5173`).

## Learning flow

- Create a topic in Kafka (or use an existing one).
- Select topic in UI and publish messages.
- Observe consumed messages from `/messages`.
- Inspect metadata from `/kafka-metadata`.
