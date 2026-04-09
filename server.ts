import express from "express";
import kafkajs from "kafkajs";
import type { Admin, Consumer, Producer, Kafka as KafkaClient } from "kafkajs";
import cors from "cors";
import bodyParser from "body-parser";
import dotenv from "dotenv";

export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string;
  value: string;
  timestamp: string;
}

dotenv.config();
const app = express();
app.use(cors());
app.use(bodyParser.json());

let kafka: KafkaClient;
let admin: Admin;
let consumer: Consumer;
let producer: Producer;
const { Kafka } = kafkajs;
let kafkaConfig = { clientId: "kafka-dashboard", brokers: ["localhost:9092"] };
// Store message count and keys per partition
let partitionData: Record<
  string,
  Record<number, { count: number; keys: Set<string> }>
> = {};

// Function to initialize Kafka with dynamic config
const initializeKafka = async () => {
  if (admin) await admin.disconnect(); // Disconnect existing admin
  kafka = new Kafka(kafkaConfig);
  producer = kafka.producer();
  consumer = kafka.consumer({ groupId: "kafka-dashboard-group" });
  admin = kafka.admin();
  await admin.connect();
};

// API to get Kafka config
app.get("/kafka-config", (req, res) => {
  res.json(kafkaConfig);
});

// API to set Kafka config and restart Kafka
app.post("/set-kafka-config", async (req, res) => {
  try {
    const { clientId, brokers } = req.body;
    kafkaConfig = { clientId, brokers: brokers.split(",") };
    await initializeKafka();
    res.json({ success: true, message: "Kafka configuration updated!" });
  } catch (error) {
    res.status(500).json({ error: "Error updating Kafka config" });
  }
});

const fetchKafkaMetadata = async () => {
  await admin.connect();

  try {
    // Fetch cluster details before disconnecting
    const clusterInfo = await admin.describeCluster();
    const brokers = clusterInfo.brokers.map((b) => b.host);
    const consumerGroups = (await admin.listGroups()).groups.map(
      (g) => g.groupId
    );

    const allTopics = await fetchAllTopics();
    const topics = allTopics.filter(
      (topic) =>
        !topic.startsWith("__") &&
        !topic.startsWith("_") &&
        topic != "transaction"
    );
    const topicMetadata = await admin.fetchTopicMetadata({ topics });
    const topicsWithPartitions = topicMetadata.topics.map((topic) => ({
      name: topic.name,
      partitions: topic.partitions.map((partition) => {
        const partitionInfo = partitionData[topic.name]?.[
          partition.partitionId
        ] || { count: 0, keys: new Set() };

        return {
          partition: partition.partitionId,
          messageCount: partitionInfo.count,
          keys: Array.from(partitionInfo.keys) // Convert Set to Array
        };
      })
    }));
    console.log(
      "topicsWithPartitions ::: ",
      JSON.stringify(topicsWithPartitions)
    );
    return { topics: topicsWithPartitions, brokers, consumerGroups };
  } catch (error) {
    console.error("Error fetching Kafka metadata:", error);
    return { topics: [], brokers: [], consumerGroups: [] };
  } finally {
    await admin.disconnect(); // Now disconnect at the end
  }
};

// Fetch Kafka metadata every 10 seconds
setInterval(fetchKafkaMetadata, 5000);

// API to Fetch Kafka Metadata
app.get("/kafka-metadata", async (req, res) => {
  try {
    const metadata = await fetchKafkaMetadata();
    res.json(metadata);
  } catch (error) {
    console.error("Error fetching Kafka metadata:", error);
    res.status(500).json({ error: "Failed to fetch Kafka metadata" });
  }
});

// Store messages in memory (or use a database for persistence)
let messages: any[] = [];
let onlyOnce = true;
let isConsumerRunning = false;

const fetchAllTopics = async () => {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  await admin.disconnect();
  return topics;
};

const fetchLatestMessages = async () => {
  try {
    if (isConsumerRunning) {
      console.warn("⚠️ Consumer is already running, skipping re-run...");
      // await consumer.disconnect();
      return;
    }
    if (onlyOnce) {
      console.log("its should call only once");
      const allTopics = await fetchAllTopics();
      const topics = allTopics.filter(
        (topic) =>
          !topic.startsWith("__") &&
          !topic.startsWith("_") &&
          topic != "transaction"
      );
      await consumer.connect();
      for (const topic of topics) {
        await consumer.subscribe({ topic, fromBeginning: true });
      }
    }
    onlyOnce = false;
    isConsumerRunning = true;
    console.log("🔄 Polling latest messages...");

    let fetchedMessages: any[] = [];

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const msgValue = message.value?.toString();
        const msgKey = message.key?.toString() || "N/A";
        const offset = message.offset;
        const timestamp = new Date(Number(message.timestamp)).toLocaleString();

        if (!partitionData[topic]) {
          partitionData[topic] = {};
        }
        if (!partitionData[topic][partition]) {
          partitionData[topic][partition] = { count: 0, keys: new Set() };
        }

        partitionData[topic][partition].count += 1;
        // partitionData[topic][partition].keys.add(msgKey); // uncomment when needed

        console.log("📥 RECEIVED MESSAGE:", {
          topic,
          partition,
          key: message.key?.toString(),
          value: message.value?.toString(),
          offset: message.offset,
          timestamp: message.timestamp
        });
        messages.push({
          topic,
          partition,
          key: message.key?.toString() || "N/A",
          value: message.value?.toString(),
          offset: message.offset,
          timestamp: message.timestamp
        });
        console.log("messages in consumer list....", messages);
      }
    });

    if (fetchedMessages.length > 0) {
      console.log("push the messages to the list....");
      messages = fetchedMessages.slice(-50); // Keep only the last 50 messages
      console.log("✅ Updated messages:", messages.length);
    }
  } catch (error) {
    console.error("❌ Error fetching latest messages:", error);
  }
};

// Poll messages every 5 seconds
setInterval(fetchLatestMessages, 5000);

app.post("/publish", async (req, res) => {
  try {
    const { topic, key, message } = req.body;
    if (!topic || !message) {
      console.error("❌ Missing topic or message:", req.body);
      res.status(400).json({ error: "Topic and message are required" });
    }
    console.log("🚀 Connecting to Kafka Producer...");
    await producer.connect();
    console.log("✅ Producer connected!");
    console.log(`📤 Sending Message: "${message}" to Topic: "${topic}"...`);
    await producer.send({
      topic,
      messages: [{ key: `key-${Date.now()}`, value: message }]
    });
    console.log("✅ Message successfully sent!");
    // await producer.disconnect();
    res.json({ success: true, message: "Message sent!" });
  } catch (error) {
    console.error("Error publishing message:", error);
    res.status(500).json({ error: "Failed to send message" });
  }
});

// API to fetch messages stored in memory
app.get("/messages", async (req, res) => {
  res.json(Array.isArray(messages) ? messages : []);
});

app.get("/topics", async (req, res) => {
  try {
    await admin.connect();
    const topics = await admin.listTopics();
    await admin.disconnect();

    res.json(Array.isArray(topics) ? topics : []); // Always return an array
  } catch (error) {
    console.error("Error fetching topics:", error);
    res.status(500).json([]); // Return an empty array on failure
  }
});

// Start server
app.listen(5000, async () => {
  await initializeKafka();
  console.log("Server running on port 5000");
  await fetchKafkaMetadata(); // Fetch immediately on startup
});
