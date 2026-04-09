import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

type KafkaMessage = {
  topic: string;
  partition: number;
  offset: string;
  key: string;
  value: string;
  timestamp: string;
};

const app = express();
app.use(cors());
app.use(express.json());

const kafka = new Kafka({
  clientId: "kafka-dashboard",
  brokers: ["localhost:19092"]
});

const producer = kafka.producer();

let producerConnected = false;
let messages: KafkaMessage[] = [];

async function getUserTopics(): Promise<string[]> {
  const localAdmin = kafka.admin();
  await localAdmin.connect();
  try {
    const topics = await localAdmin.listTopics();
    return topics.filter((t) => !t.startsWith("_") && !t.startsWith("__"));
  } finally {
    await localAdmin.disconnect();
  }
}

async function ensureProducerConnected() {
  if (producerConnected) return;
  await producer.connect();
  producerConnected = true;
}

app.get("/topics", async (_req, res) => {
  try {
    const topics = await getUserTopics();
    res.json(topics);
  } catch (error) {
    res.status(500).json({ error: "Failed to list topics", details: `${error}` });
  }
});

app.post("/topics", async (req, res) => {
  const { topic, numPartitions = 1, replicationFactor = 1 } = req.body as {
    topic?: string;
    numPartitions?: number;
    replicationFactor?: number;
  };

  if (!topic?.trim()) {
    return res.status(400).json({ error: "topic is required" });
  }

  const localAdmin = kafka.admin();
  await localAdmin.connect();
  try {
    const existingTopics = await localAdmin.listTopics();
    if (existingTopics.includes(topic.trim())) {
      return res.json({ success: true, created: false, message: "Topic already exists" });
    }

    const created = await localAdmin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: topic.trim(),
          numPartitions,
          replicationFactor
        }
      ]
    });
    return res.json({ success: true, created });
  } catch (error) {
    return res.status(500).json({ error: "Failed to create topic", details: `${error}` });
  } finally {
    await localAdmin.disconnect();
  }
});

app.get("/messages", (_req, res) => {
  res.json(messages);
});

app.get("/kafka-metadata", async (_req, res) => {
  const localAdmin = kafka.admin();
  await localAdmin.connect();
  try {
    const cluster = await localAdmin.describeCluster();
    const groups = await localAdmin.listGroups();
    const topics = (await localAdmin.listTopics()).filter(
      (t) => !t.startsWith("_") && !t.startsWith("__")
    );
    const topicMetadata = topics.length
      ? await localAdmin.fetchTopicMetadata({ topics })
      : { topics: [] };

    res.json({
      brokers: cluster.brokers.map((b) => b.host),
      consumerGroups: groups.groups.map((g) => g.groupId),
      topics: topicMetadata.topics.map((t) => ({
        name: t.name,
        partitions: t.partitions.map((p) => ({ partition: p.partitionId }))
      }))
    });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch metadata", details: `${error}` });
  } finally {
    await localAdmin.disconnect();
  }
});

app.post("/publish", async (req, res) => {
  const { topic, message, key } = req.body as {
    topic?: string;
    message?: string;
    key?: string;
  };

  if (!topic || !message) {
    return res.status(400).json({ error: "topic and message are required" });
  }

  try {
    await ensureProducerConnected();
    const result = await producer.send({
      topic,
      messages: [{ key: key ?? `key-${Date.now()}`, value: message }]
    });
    const metadata = result[0];
    messages.push({
      topic,
      partition: metadata.partition,
      offset: metadata.baseOffset ?? "0",
      key: key ?? "N/A",
      value: message,
      timestamp: new Date().toISOString()
    });
    if (messages.length > 200) messages = messages.slice(-200);
    return res.json({ success: true });
  } catch (error) {
    return res.status(500).json({ error: "Failed to publish", details: `${error}` });
  }
});

const PORT = 5000;
app.listen(PORT, async () => {
  console.log(`Server listening on ${PORT}`);
});
