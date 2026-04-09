const BASE_URL = "http://localhost:5000";

export type KafkaMessage = {
  topic: string;
  partition: number;
  offset: string;
  key: string;
  value: string;
  timestamp: string;
};

export async function getTopics(): Promise<string[]> {
  const res = await fetch(`${BASE_URL}/topics`);
  if (!res.ok) throw new Error("Failed to fetch topics");
  return res.json();
}

export async function createTopic(payload: {
  topic: string;
  numPartitions?: number;
  replicationFactor?: number;
}): Promise<void> {
  const res = await fetch(`${BASE_URL}/topics`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error("Failed to create topic");
}

export async function getMessages(): Promise<KafkaMessage[]> {
  const res = await fetch(`${BASE_URL}/messages`);
  if (!res.ok) throw new Error("Failed to fetch messages");
  return res.json();
}

export async function publishMessage(payload: {
  topic: string;
  message: string;
  key?: string;
}): Promise<void> {
  const res = await fetch(`${BASE_URL}/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error("Failed to publish message");
}
