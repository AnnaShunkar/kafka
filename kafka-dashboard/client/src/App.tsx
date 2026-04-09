import { useEffect, useState } from "react";
import {
  Box,
  Button,
  Container,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import {
  createTopic,
  getMessages,
  getTopics,
  publishMessage,
  type KafkaMessage
} from "./api";

export default function App() {
  const [topics, setTopics] = useState<string[]>([]);
  const [topic, setTopic] = useState("");
  const [newTopic, setNewTopic] = useState("");
  const [message, setMessage] = useState("");
  const [messages, setMessages] = useState<KafkaMessage[]>([]);

  const loadTopics = async () => {
    try {
      const fetchedTopics = await getTopics();
      setTopics(fetchedTopics);
      if (fetchedTopics.length && !fetchedTopics.includes(topic)) {
        setTopic(fetchedTopics[0]);
      }
    } catch {
      setTopics([]);
    }
  };

  useEffect(() => {
    loadTopics();
  }, []);

  useEffect(() => {
    const loadMessages = async () => {
      try {
        setMessages(await getMessages());
      } catch {
        setMessages([]);
      }
    };
    loadMessages();
    const timer = setInterval(loadMessages, 3000);
    return () => clearInterval(timer);
  }, []);

  const onSend = async () => {
    if (!topic || !message.trim()) return;
    await publishMessage({ topic, message });
    setMessage("");
  };

  const onCreateTopic = async () => {
    if (!newTopic.trim()) return;
    await createTopic({ topic: newTopic.trim() });
    setNewTopic("");
    await loadTopics();
    setTopic(newTopic.trim());
  };

  return (
    <Container maxWidth="md">
      <Box py={6}>
        <Typography variant="h4" gutterBottom>
          Kafka Dashboard
        </Typography>
        <Stack direction="row" spacing={2} mb={2}>
          <TextField
            label="Create Topic"
            value={newTopic}
            onChange={(e) => setNewTopic(e.target.value)}
            sx={{ minWidth: 280 }}
          />
          <Button variant="outlined" onClick={onCreateTopic}>
            Create
          </Button>
        </Stack>
        <Stack direction="row" spacing={2} mb={3}>
          <FormControl sx={{ minWidth: 220 }}>
            <InputLabel id="topic-select-label">Topic</InputLabel>
            <Select
              labelId="topic-select-label"
              label="Topic"
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
            >
              {topics.map((t) => (
                <MenuItem key={t} value={t}>
                  {t}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            fullWidth
            value={message}
            placeholder="Write a message..."
            onChange={(e) => setMessage(e.target.value)}
          />
          <Button variant="contained" onClick={onSend}>
            Publish
          </Button>
        </Stack>

        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" mb={1}>
            Latest Messages
          </Typography>
          {messages.slice(-10).reverse().map((m, idx) => (
            <Box key={`${m.topic}-${m.offset}-${idx}`} mb={1}>
              <Typography variant="body2">
                [{m.topic}] partition {m.partition} offset {m.offset}
              </Typography>
              <Typography variant="body1">{m.value}</Typography>
            </Box>
          ))}
          {!messages.length && (
            <Typography color="text.secondary">
              No messages yet. Publish one from above.
            </Typography>
          )}
        </Paper>
      </Box>
    </Container>
  );
}
