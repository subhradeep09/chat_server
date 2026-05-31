const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const mongoose = require('mongoose');
const { Server } = require('socket.io');

dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const PORT = process.env.CHAT_SERVER_PORT || 3001;
const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || 'puja_chat';
const CLIENT_ORIGIN = process.env.CHAT_CLIENT_ORIGIN || '*';

if (!MONGODB_URI) {
  console.error('Missing MONGODB_URI in .env');
  process.exit(1);
}

if (/atlas-sql|\.query\.mongodb\.net/i.test(MONGODB_URI)) {
  console.error(
    'MONGODB_URI points to an Atlas SQL endpoint, but this server uses Mongoose and needs a MongoDB driver URI. Use the Atlas Connect \u2192 Drivers string (mongodb+srv://USER:PASS@cluster.../chat_puja?retryWrites=true&w=majority).'
  );
  process.exit(1);
}

const messageSchema = new mongoose.Schema(
  {
    chatId: { type: String, required: true, index: true },
    senderId: { type: String, required: true, index: true },
    senderName: { type: String, required: true },
    receiverId: { type: String, required: true, index: true },
    text: { type: String, required: true, trim: true },
    clientMessageId: { type: String, default: null, index: true },
    deliveredAt: { type: Date, default: Date.now },
    readAt: { type: Date, default: null },
  },
  {
    timestamps: { createdAt: true, updatedAt: false },
    versionKey: false,
  }
);

messageSchema.index({ chatId: 1, createdAt: 1 });

const Message = mongoose.model('Message', messageSchema);

function serializeMessage(message) {
  return {
    id: String(message._id),
    chatId: message.chatId,
    senderId: message.senderId,
    senderName: message.senderName,
    receiverId: message.receiverId,
    text: message.text,
    clientMessageId: message.clientMessageId || null,
    createdAt: message.createdAt,
    deliveredAt: message.deliveredAt,
    readAt: message.readAt,
  };
}

async function loadMessages(chatId) {
  const messages = await Message.find({ chatId }).sort({ createdAt: 1 }).lean();
  return messages.map((message) => serializeMessage(message));
}

async function createMessage(payload) {
  const doc = await Message.create(payload);
  return serializeMessage(doc);
}

async function markConversationRead(chatId, readerId, readAt) {
  await Message.updateMany(
    {
      chatId,
      senderId: { $ne: readerId },
      readAt: null,
    },
    {
      $set: { readAt },
    }
  );
}

async function main() {
  await mongoose.connect(MONGODB_URI, {
    dbName: MONGODB_DB,
    serverSelectionTimeoutMS: 5000,
  });

  mongoose.connection.on('connected', () => {
    console.log('MongoDB connected to', MONGODB_DB);
  });

  mongoose.connection.on('error', (err) => {
    console.error('MongoDB connection error:', err);
  });

  const app = express();
  const server = http.createServer(app);
  const io = new Server(server, {
    cors: {
      origin: CLIENT_ORIGIN,
      methods: ['GET', 'POST'],
    },
  });

  app.use(cors());
  app.use(express.json());

  app.get('/health', (_req, res) => {
    res.json({ ok: true });
  });

  app.get('/messages', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || '').trim();
      if (!chatId) {
        return res.status(400).json({ error: 'chatId is required' });
      }

      const messages = await loadMessages(chatId);
      res.json(messages);
    } catch (error) {
      console.error('GET /messages error:', error);
      res.status(500).json({ error: 'Unable to load messages' });
    }
  });

  app.post('/messages', async (req, res) => {
    try {
      const { chatId, senderId, senderName, receiverId, text, clientMessageId } = req.body || {};

      if (!chatId || !senderId || !receiverId || !text) {
        return res.status(400).json({ error: 'chatId, senderId, receiverId, and text are required' });
      }

      const message = await createMessage({
        chatId,
        senderId,
        senderName: senderName || 'Unknown',
        receiverId,
        text,
        clientMessageId: clientMessageId || null,
        deliveredAt: new Date(),
      });

      io.to(chatId).emit('message:new', message);
      res.status(201).json(message);
    } catch (error) {
      console.error('POST /messages error:', error);
      res.status(500).json({ error: 'Unable to save message' });
    }
  });

  io.on('connection', (socket) => {
    const chatId = String(socket.handshake.query.chatId || '').trim();
    if (chatId) {
      socket.join(chatId);
    }

    socket.on('chat:join', ({ chatId: roomId }) => {
      if (roomId) {
        socket.join(roomId);
      }
    });

    socket.on('typing:status', (payload) => {
      const { chatId: roomId, userId, userName, isTyping } = payload || {};
      if (!roomId || !userId) {
        return;
      }

      socket.to(roomId).emit('typing:status', {
        chatId: roomId,
        userId,
        userName: userName || 'Someone',
        isTyping: Boolean(isTyping),
      });
    });

    socket.on('chat:read', async (payload) => {
      try {
        const { chatId: roomId, readerId, readerName } = payload || {};
        if (!roomId || !readerId) {
          return;
        }

        const readAt = new Date();
        await markConversationRead(roomId, readerId, readAt);

        socket.to(roomId).emit('chat:read', {
          chatId: roomId,
          readerId,
          readerName: readerName || 'Reader',
          readAt,
        });
      } catch (error) {
        console.error('chat:read error:', error);
      }
    });

    socket.on('message:send', async (payload, ack) => {
      try {
        const { chatId: roomId, senderId, senderName, receiverId, text, clientMessageId } = payload || {};

        if (!roomId || !senderId || !receiverId || !text) {
          if (typeof ack === 'function') {
            ack({ ok: false, error: 'Missing required message fields' });
          }
          return;
        }

        const message = await createMessage({
          chatId: roomId,
          senderId,
          senderName: senderName || 'Unknown',
          receiverId,
          text,
          clientMessageId: clientMessageId || null,
          deliveredAt: new Date(),
        });

        io.to(roomId).emit('message:new', message);

        if (typeof ack === 'function') {
          ack({ ok: true, message });
        }
      } catch (error) {
        console.error('socket message:send error:', error);
        if (typeof ack === 'function') {
          ack({ ok: false, error: 'Unable to save message' });
        }
      }
    });
  });

  server.listen(PORT, () => {
    console.log(`Chat server listening on http://localhost:${PORT}`);
  });
}

main().catch((error) => {
  console.error('Chat server failed to start:', error);
  process.exit(1);
});
