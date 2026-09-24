const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const mongoose = require('mongoose');
const { Server } = require('socket.io');
const admin = require('firebase-admin');

dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const PORT         = process.env.PORT || process.env.CHAT_SERVER_PORT || 3001;
const MONGODB_URI  = process.env.MONGODB_URI;
const MONGODB_DB   = process.env.MONGODB_DB || 'puja_chat';
const CLIENT_ORIGIN = process.env.CHAT_CLIENT_ORIGIN || '*';

// ─── Firebase Admin (FCM) setup ───────────────────────────────────────────────
// On Render: set FIREBASE_SERVICE_ACCOUNT_JSON env var with the full JSON string
// Locally:   place serviceAccountKey.json in the server/ folder
let firebaseApp = null;
try {
  let serviceAccount;
  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    // Render / production: read credentials from environment variable
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
    console.log('[FCM] Using service account from FIREBASE_SERVICE_ACCOUNT_JSON env var');
  } else {
    // Local dev: read from file
    serviceAccount = require(path.resolve(__dirname, 'serviceAccountKey.json'));
    console.log('[FCM] Using service account from serviceAccountKey.json file');
  }
  firebaseApp = admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
  console.log('[FCM] Firebase Admin initialized ✅');
} catch (e) {
  console.error('[FCM] ❌ Firebase Admin NOT initialized:', e.message);
  console.error('[FCM]    → On Render: add FIREBASE_SERVICE_ACCOUNT_JSON environment variable');
  console.error('[FCM]    → Locally: place serviceAccountKey.json in the server/ folder');
}

/**
 * Send FCM push notification directly via firebase-admin (no Expo intermediary).
 * fcmToken = raw device token from Notifications.getDevicePushTokenAsync() on Android.
 */
async function sendFCMNotification(fcmToken, title, body, data = {}) {
  if (!firebaseApp) {
    console.warn('[FCM] Skipping push — firebase-admin not initialized');
    return;
  }
  if (!fcmToken) {
    console.warn('[FCM] Skipping push — no FCM token for this user');
    return;
  }
  try {
    // FCM data values must all be strings
    const stringData = {};
    for (const [k, v] of Object.entries(data)) stringData[k] = String(v);

    const result = await admin.messaging().send({
      token: fcmToken,
      notification: { title, body },
      data: stringData,
      android: {
        priority: 'high',
        notification: {
          channelId: 'chat',
          sound: 'default',
          defaultVibrateTimings: true,
        },
      },
    });
    console.log('[FCM] ✅ Notification sent:', result);
  } catch (err) {
    console.error('[FCM] ❌ Send failed:', err.message);
    if (err.code === 'messaging/registration-token-not-registered') {
      console.warn('[FCM] Token expired — user needs to reopen app to refresh');
    }
  }
}


if (!MONGODB_URI) {
  console.error('Missing MONGODB_URI in .env');
  process.exit(1);
}

if (/atlas-sql|\.query\.mongodb\.net/i.test(MONGODB_URI)) {
  console.error('MONGODB_URI points to an Atlas SQL endpoint. Use the Drivers connection string instead.');
  process.exit(1);
}

// ─── Mongoose schemas ─────────────────────────────────────────────────────────

// ── 1. messages collection (Personal 1-on-1 Messages) ──
const messageSchema = new mongoose.Schema(
  {
    chatId:          { type: String, required: true, index: true },
    senderId:        { type: String, required: true, index: true },
    senderName:      { type: String, required: true },
    receiverId:      { type: String, required: true, index: true },
    text:            { type: String, required: true, trim: true },
    clientMessageId: { type: String, default: null, index: true },
    deliveredAt:     { type: Date, default: Date.now },
    readAt:          { type: Date, default: null },
    replyTo:         { type: mongoose.Schema.Types.Mixed, default: null },
    isEdited:        { type: Boolean, default: false },
    editedAt:        { type: Date, default: null },
    reactions:       { type: Array, default: [] },
  },
  { timestamps: { createdAt: true, updatedAt: false }, versionKey: false, collection: 'messages' }
);
messageSchema.index({ chatId: 1, createdAt: 1 });
messageSchema.index({ chatId: 1, createdAt: -1 });
const Message = mongoose.model('Message', messageSchema);

// ── 2. group_massages collection (Group Chat Messages) ──
const groupMessageSchema = new mongoose.Schema(
  {
    chatId:          { type: String, required: true, index: true },
    groupId:         { type: String, required: true, index: true },
    groupName:       { type: String, default: '' },
    senderId:        { type: String, required: true, index: true },
    senderName:      { type: String, required: true },
    text:            { type: String, required: true, trim: true },
    clientMessageId: { type: String, default: null, index: true },
    deliveredAt:     { type: Date, default: Date.now },
    readAt:          { type: Date, default: null },
    replyTo:         { type: mongoose.Schema.Types.Mixed, default: null },
    isEdited:        { type: Boolean, default: false },
    editedAt:        { type: Date, default: null },
    reactions:       { type: Array, default: [] },
  },
  { timestamps: { createdAt: true, updatedAt: false }, versionKey: false, collection: 'group_massages' }
);
groupMessageSchema.index({ chatId: 1, createdAt: 1 });
groupMessageSchema.index({ groupId: 1, createdAt: -1 });
const GroupMessage = mongoose.model('GroupMessage', groupMessageSchema);

// ── 3. group_notification collection (Group Notifications & System Events) ──
const groupNotificationSchema = new mongoose.Schema(
  {
    chatId:          { type: String, required: true, index: true },
    groupId:         { type: String, required: true, index: true },
    type:            { type: String, required: true, index: true }, // 'system' | 'invite' | 'invite_accepted' | 'invite_rejected' | 'member_sync' | 'member_left' | 'member_kicked' | 'location' | 'location_stopped'
    senderId:        { type: String, default: null },
    senderName:      { type: String, default: null },
    receiverId:      { type: String, default: 'all' },
    text:            { type: String, required: true },
    data:            { type: mongoose.Schema.Types.Mixed, default: null },
    clientMessageId: { type: String, default: null, index: true },
  },
  { timestamps: { createdAt: true, updatedAt: false }, versionKey: false, collection: 'group_notification' }
);
groupNotificationSchema.index({ chatId: 1, createdAt: -1 });
groupNotificationSchema.index({ groupId: 1, type: 1 });
const GroupNotification = mongoose.model('GroupNotification', groupNotificationSchema);

// ── 4. massage_notification collection (Message & 1-on-1 Notifications / System Events) ──
// Stores message notification events (e.g. location share off, group invites & status in 1-on-1 chats)
const massageNotificationSchema = new mongoose.Schema(
  {
    chatId:          { type: String, required: true, index: true },
    type:            { type: String, required: true, index: true }, // 'location_stopped' | 'invite' | 'invite_accepted' | 'invite_rejected' | 'member_sync' | 'member_left' | 'member_kicked' | 'location' | 'location_stopped' | 'system' | 'general'
    senderId:        { type: String, default: null, index: true },
    senderName:      { type: String, default: null },
    receiverId:      { type: String, default: null, index: true },
    groupId:         { type: String, default: null, index: true },
    groupName:       { type: String, default: '' },
    text:            { type: String, required: true },
    data:            { type: mongoose.Schema.Types.Mixed, default: null },
    clientMessageId: { type: String, default: null, index: true },
    deliveredAt:     { type: Date, default: Date.now },
    readAt:          { type: Date, default: null },
  },
  { timestamps: { createdAt: true, updatedAt: false }, versionKey: false, collection: 'massage_notification' }
);
massageNotificationSchema.index({ chatId: 1, createdAt: -1 });
massageNotificationSchema.index({ receiverId: 1, type: 1 });
massageNotificationSchema.index({ groupId: 1, createdAt: -1 });
const MassageNotification = mongoose.model('MassageNotification', massageNotificationSchema);

// ── 4. notifytoken collection ──
// Stores Expo push tokens permanently in MongoDB (collection: notifytoken).
// One document per user — upserted on every app login.
// Survives server restarts unlike the old in-memory Map.
const notifyTokenSchema = new mongoose.Schema(
  {
    userId: { type: String, required: true, unique: true, index: true },
    token:  { type: String, required: true },
  },
  { timestamps: { createdAt: true, updatedAt: true }, versionKey: false, collection: 'notifytoken' }
);
const NotifyToken = mongoose.model('NotifyToken', notifyTokenSchema);

// ── notifytoken helpers ──

/** Save (upsert) a push token for a user — called when app registers */
async function savePushToken(userId, token) {
  await NotifyToken.findOneAndUpdate(
    { userId },
    { userId, token },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );
}

/** Get the push token for a user — called before sending a notification */
async function getPushToken(userId) {
  const doc = await NotifyToken.findOne({ userId }).lean();
  return doc ? doc.token : null;
}

/** Delete the push token for a user — called when user clears notifications */
async function deletePushToken(userId) {
  await NotifyToken.deleteOne({ userId });
}

// ── Helpers for packet classification & serialization ──

function isNotificationPacket(text) {
  if (!text) return false;
  return typeof text === 'string' && text.startsWith('[GROUP_');
}

function isMassageNotificationPacket(text) {
  if (!text || typeof text !== 'string') return false;
  return (
    text.startsWith('[GROUP_') ||
    text.startsWith('[LIVE_LOCATION_STOPPED]:') ||
    text.startsWith('[LOCATION_STOPPED]:')
  );
}

function parseMassageNotification(text, chatId, payload = {}) {
  let type = 'general';
  let data = null;
  let groupId = null;
  let groupName = payload.groupName || payload.group_name || '';

  if (typeof text === 'string') {
    const match = text.match(/^\[([A-Z_]+)\]:(.*)$/s);
    if (match) {
      const rawTag = match[1];
      const payloadStr = match[2];
      if (rawTag.startsWith('GROUP_')) {
        type = rawTag.replace(/^GROUP_/, '').toLowerCase();
      } else if (rawTag.includes('LOCATION_STOPPED')) {
        type = 'location_stopped';
      } else {
        type = rawTag.toLowerCase();
      }
      try {
        data = JSON.parse(payloadStr);
      } catch {
        data = { raw: payloadStr };
      }
      if (data && typeof data === 'object') {
        groupId = data.groupId || null;
        groupName = data.groupName || groupName;
      }
    }
  }
  if (!groupId && chatId && chatId.startsWith('group_')) {
    groupId = chatId.replace(/^group_/, '');
  }
  return { type, data, groupId, groupName };
}

function parseNotificationPayload(text, chatId) {
  let type = 'general';
  let data = null;
  const match = typeof text === 'string' ? text.match(/^\[(GROUP_[A-Z_]+)\]:(.*)$/s) : null;
  if (match) {
    const rawTag = match[1];
    const payloadStr = match[2];
    type = rawTag.replace(/^GROUP_/, '').toLowerCase();
    try {
      data = JSON.parse(payloadStr);
    } catch {
      data = { raw: payloadStr };
    }
  }
  const groupId = (chatId && chatId.startsWith('group_'))
    ? chatId.replace(/^group_/, '')
    : (data?.groupId || null);

  return { type, data, groupId };
}

function serializeRecord(doc) {
  return {
    id:              String(doc._id),
    chatId:          doc.chatId,
    groupId:         doc.groupId || (doc.chatId && doc.chatId.startsWith('group_') ? doc.chatId.replace(/^group_/, '') : null),
    groupName:       doc.groupName || null,
    senderId:        doc.senderId || null,
    senderName:      doc.senderName || null,
    receiverId:      doc.receiverId || (doc.chatId && doc.chatId.startsWith('group_') ? 'all' : null),
    text:            doc.text,
    type:            doc.type || null,
    data:            doc.data || null,
    clientMessageId: doc.clientMessageId || null,
    replyTo:         doc.replyTo || null,
    isEdited:        Boolean(doc.isEdited),
    editedAt:        doc.editedAt || null,
    reactions:       Array.isArray(doc.reactions) ? doc.reactions : [],
    createdAt:       doc.createdAt,
    deliveredAt:     doc.deliveredAt || doc.createdAt,
    readAt:          doc.readAt || null,
  };
}

async function saveIncomingPacket(payload) {
  const { chatId, senderId, senderName, receiverId, text, clientMessageId, replyTo } = payload;
  const isGroup = chatId && chatId.startsWith('group_');
  const isMassageNotif = isMassageNotificationPacket(text) || payload.isNotification;

  // 1. All notification packets (location share off, groups info in messages, etc.) -> massage_notification collection
  if (isMassageNotif) {
    const { type, data, groupId, groupName } = parseMassageNotification(text, chatId, payload);
    const doc = await MassageNotification.create({
      chatId,
      type,
      senderId: senderId || null,
      senderName: senderName || 'Unknown',
      receiverId: receiverId || (isGroup ? 'all' : null),
      groupId: groupId || (isGroup ? chatId.replace(/^group_/, '') : null),
      groupName: groupName || '',
      text,
      data,
      clientMessageId: clientMessageId || null,
      deliveredAt: new Date(),
    });

    // Only persist to GroupNotification if it's an actual group room (starts with group_)
    if (isGroup) {
      try {
        await GroupNotification.create({
          chatId,
          groupId: groupId || chatId.replace(/^group_/, ''),
          type,
          senderId: senderId || null,
          senderName: senderName || 'System',
          receiverId: receiverId || 'all',
          text,
          data,
          clientMessageId: clientMessageId || null,
        });
      } catch (e) {}
    }

    return { record: serializeRecord(doc), category: 'massage_notification' };
  }

  // 2. Group chat messages -> group_massages collection
  if (isGroup) {
    const groupId = chatId.replace(/^group_/, '');
    let groupName = payload.groupName || payload.group_name || '';

    // If groupName was not provided in the payload, look it up from massage_notification or group_notification
    if (!groupName) {
      try {
        const knownNotif = await MassageNotification.findOne({
          groupId,
          'data.groupName': { $exists: true, $ne: '' }
        }).lean() || await GroupNotification.findOne({
          groupId,
          'data.groupName': { $exists: true, $ne: '' }
        }).lean();
        if (knownNotif?.data?.groupName) {
          groupName = knownNotif.data.groupName;
        }
      } catch (e) {}
    }

    const doc = await GroupMessage.create({
      chatId,
      groupId,
      groupName,
      senderId,
      senderName: senderName || 'Unknown',
      text,
      clientMessageId: clientMessageId || null,
      replyTo: replyTo || null,
      deliveredAt: new Date(),
    });
    return { record: serializeRecord(doc), category: 'group_message' };
  }

  // 3. Regular 1-on-1 personal chat messages -> messages collection
  const doc = await Message.create({
    chatId,
    senderId,
    senderName: senderName || 'Unknown',
    receiverId,
    text,
    clientMessageId: clientMessageId || null,
    replyTo: replyTo || null,
    deliveredAt: new Date(),
  });
  return { record: serializeRecord(doc), category: 'message' };
}

async function editMessageInDb({ chatId, messageId, clientMessageId, newText, senderId }) {
  if (!chatId || !newText) return null;
  const isGroup = chatId.startsWith('group_');
  const Model = isGroup ? GroupMessage : Message;

  let doc = null;
  if (messageId && mongoose.isValidObjectId(messageId)) {
    doc = await Model.findById(messageId);
  }
  if (!doc && clientMessageId) {
    doc = await Model.findOne({ chatId, clientMessageId });
  }
  if (!doc && messageId) {
    doc = await Model.findOne({ chatId, clientMessageId: messageId });
  }
  if (!doc) return null;

  doc.text = newText.trim();
  doc.isEdited = true;
  doc.editedAt = new Date();
  await doc.save();
  return serializeRecord(doc);
}

async function reactToMessageInDb({ chatId, messageId, clientMessageId, userId, userName, emoji }) {
  if (!chatId || !userId || !emoji) return null;
  const isGroup = chatId.startsWith('group_');
  const Model = isGroup ? GroupMessage : Message;

  let doc = null;
  if (messageId && mongoose.isValidObjectId(messageId)) {
    doc = await Model.findById(messageId);
  }
  if (!doc && clientMessageId) {
    doc = await Model.findOne({ chatId, clientMessageId });
  }
  if (!doc && messageId) {
    doc = await Model.findOne({ chatId, clientMessageId: messageId });
  }
  if (!doc) return null;

  let reactions = Array.isArray(doc.reactions) ? [...doc.reactions] : [];
  const existingIdx = reactions.findIndex((r) => r.userId === userId);

  if (existingIdx >= 0) {
    if (reactions[existingIdx].emoji === emoji) {
      // Toggle off
      reactions.splice(existingIdx, 1);
    } else {
      // Switch emoji
      reactions[existingIdx] = { userId, userName: userName || '', emoji };
    }
  } else {
    reactions.push({ userId, userName: userName || '', emoji });
  }

  doc.reactions = reactions;
  doc.markModified('reactions');
  await doc.save();
  return serializeRecord(doc);
}

async function loadMessages(chatId, limit = 100) {
  const parsedLimit = Math.min(Math.max(parseInt(limit, 10) || 100, 1), 500);
  if (chatId.startsWith('group_')) {
    // Return group chat messages, massage_notifications, and system announcements for GroupChatScreen history
    const [groupMsgs, sysNotifs, massageNotifs] = await Promise.all([
      GroupMessage.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
      GroupNotification.find({ chatId, type: { $in: ['system', 'general'] } }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
      MassageNotification.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
    ]);
    const seen = new Set();
    const combined = [];
    for (const item of [...groupMsgs, ...sysNotifs, ...massageNotifs]) {
      const key = item.clientMessageId || String(item._id);
      if (!seen.has(key)) {
        seen.add(key);
        combined.push(item);
      }
    }
    combined.sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
    return combined.slice(-parsedLimit).map(serializeRecord);
  } else {
    // Personal 1-on-1 direct messages (include Message and MassageNotification)
    const [msgs, massageNotifs] = await Promise.all([
      Message.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
      MassageNotification.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
    ]);
    const seen = new Set();
    const seenGroupInvites = new Set();
    const combined = [];
    for (const item of [...msgs, ...massageNotifs]) {
      // Deduplicate group invite cards by groupId
      if (item.text?.startsWith('[GROUP_INVITE]:')) {
        try {
          const inv = JSON.parse(item.text.replace('[GROUP_INVITE]:', ''));
          if (inv?.groupId) {
            if (seenGroupInvites.has(inv.groupId)) continue;
            seenGroupInvites.add(inv.groupId);
          }
        } catch {}
      }

      const key = item.clientMessageId || String(item._id);
      if (!seen.has(key)) {
        seen.add(key);
        combined.push(item);
      }
    }
    combined.sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
    return combined.slice(-parsedLimit).map(serializeRecord);
  }
}

async function markConversationRead(chatId, readerId, readAt) {
  if (!chatId || !readerId) return;
  const ts = readAt || new Date();
  if (chatId.startsWith('group_')) {
    await Promise.all([
      GroupMessage.updateMany(
        { chatId, senderId: { $ne: readerId } },
        { $set: { readAt: ts, status: 'read' } }
      ),
      MassageNotification.updateMany(
        { chatId, senderId: { $ne: readerId } },
        { $set: { readAt: ts, status: 'read' } }
      ),
    ]);
  } else {
    await Promise.all([
      Message.updateMany(
        { chatId, senderId: { $ne: readerId } },
        { $set: { readAt: ts, status: 'read' } }
      ),
      MassageNotification.updateMany(
        { chatId, senderId: { $ne: readerId } },
        { $set: { readAt: ts, status: 'read' } }
      ),
      GroupNotification.updateMany(
        { chatId, senderId: { $ne: readerId } },
        { $set: { readAt: ts, status: 'read' } }
      ),
    ]);
  }
}

// ─── In-memory presence store ─────────────────────────────────────────────────
// userPresence: Map<userId, { socketId, isActive, chatId, userName, lastSeen }>
const userPresence = new Map();

// socketMeta: Map<socketId, { userId, chatId, mode }>
// mode: 'chat' (in a specific chat) | 'background' (global listener on HomeScreen)
const socketMeta = new Map();



function setUserOnline(userId, socketId, chatId, userName) {
  userPresence.set(userId, {
    socketId,
    isActive: true,
    chatId: chatId || null,
    userName: userName || userId,
    lastSeen: new Date(),
  });
}

function setUserOffline(userId) {
  const existing = userPresence.get(userId);
  if (existing) {
    userPresence.set(userId, { ...existing, isActive: false, lastSeen: new Date() });
  }
}

function isUserOnline(userId) {
  const p = userPresence.get(userId);
  return p ? p.isActive : false;
}

async function main() {
  await mongoose.connect(MONGODB_URI, {
    dbName: MONGODB_DB,
    serverSelectionTimeoutMS: 5000,
  });

  mongoose.connection.on('connected', () => console.log('MongoDB connected to', MONGODB_DB));
  mongoose.connection.on('error', (err) => console.error('MongoDB error:', err));

  const app    = express();
  const server = http.createServer(app);
  const io     = new Server(server, {
    cors: { origin: CLIENT_ORIGIN, methods: ['GET', 'POST'] },
  });

  app.use(cors());
  app.use(express.json());

  app.get('/',       (_req, res) => res.json({ ok: true, service: 'puja-app-chat-server' }));
  app.get('/health', (_req, res) => res.json({ ok: true }));

  // ── REST: register / update push token ──────────────────────────────────────
  // Accepts raw FCM device tokens (from getDevicePushTokenAsync on Android).
  // Upserts into MongoDB notifytoken collection.
  app.post('/push-token', async (req, res) => {
    try {
      const { userId, token } = req.body || {};
      if (!userId || !token) {
        return res.status(400).json({ error: 'userId and token are required' });
      }
      if (typeof token !== 'string' || token.length < 10) {
        return res.status(400).json({ error: 'Invalid token format' });
      }
      await savePushToken(userId, token);
      console.log(`[push-token] Saved to MongoDB for userId=${userId}, token=${token.slice(0, 20)}...`);
      res.json({ ok: true });
    } catch (err) {
      console.error('[push-token] POST error:', err);
      res.status(500).json({ error: 'Failed to save push token' });
    }
  });

  // ── REST: clear push token (when user sees/clears notifications) ─────────────
  // Removes the document from notifytoken collection in MongoDB.
  app.delete('/push-token/:userId', async (req, res) => {
    try {
      await deletePushToken(req.params.userId);
      console.log(`[push-token] Deleted from MongoDB for userId=${req.params.userId}`);
      res.json({ ok: true });
    } catch (err) {
      console.error('[push-token] DELETE error:', err);
      res.status(500).json({ error: 'Failed to delete push token' });
    }
  });

  // ── REST: DEBUG — check if a token exists for a userId ─────────────────────
  // Visit: GET /push-token/:userId to verify token saved in MongoDB
  app.get('/push-token/:userId', async (req, res) => {
    try {
      const token = await getPushToken(req.params.userId);
      if (token) {
        res.json({ ok: true, hasToken: true, token });
      } else {
        res.json({ ok: true, hasToken: false, token: null });
      }
    } catch (err) {
      res.status(500).json({ error: 'Failed to check push token' });
    }
  });

  // ── REST: load messages ──────────────────────────────────────────────────────
  app.get('/messages', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || '').trim();
      if (!chatId) return res.status(400).json({ error: 'chatId is required' });
      const limit = req.query.limit ? parseInt(req.query.limit, 10) : 100;
      res.json(await loadMessages(chatId, limit));
    } catch (err) {
      console.error('GET /messages error:', err);
      res.status(500).json({ error: 'Unable to load messages' });
    }
  });

  // ── REST: send message / packet (smart routing across the 3 message schemas) ─
  app.post('/messages', async (req, res) => {
    try {
      const { chatId, senderId, senderName, receiverId, text, clientMessageId } = req.body || {};
      if (!chatId || !senderId || !text)
        return res.status(400).json({ error: 'chatId, senderId, and text are required' });

      const { record, category } = await saveIncomingPacket({
        chatId,
        senderId,
        senderName: senderName || 'Unknown',
        receiverId: receiverId || (chatId.startsWith('group_') ? 'all' : null),
        text,
        clientMessageId: clientMessageId || null,
      });

      // Deliver to chat room members
      io.to(chatId).emit('message:new', record);
      if (category === 'group_message') {
        io.to(chatId).emit('group_message:new', record);
      } else if (category === 'group_notification') {
        io.to(chatId).emit('group_notification:new', record);
      }

      // Also push to receiver's background socket on ANY page (HomeScreen, Friends, etc.)
      if (receiverId && receiverId !== 'all') {
        const sentUserIds = new Set();
        for (const [sid, meta] of socketMeta.entries()) {
          if (meta.userId === receiverId && meta.chatId !== chatId && !sentUserIds.has(meta.userId)) {
            const destSock = io.sockets.sockets.get(sid);
            if (destSock) {
              destSock.emit('message:new', record);
              sentUserIds.add(meta.userId);
              console.log(`[POST /messages] Forwarded message:new to receiver socket sid=${sid} (mode=${meta.mode})`);
            }
          }
        }
      }

      res.status(201).json(record);
    } catch (err) {
      console.error('POST /messages error:', err);
      res.status(500).json({ error: 'Unable to save message' });
    }
  });

  // ── REST: edit message ─────────────────────────────────────────────────────
  app.post('/messages/edit', async (req, res) => {
    try {
      const { chatId, messageId, clientMessageId, newText, senderId } = req.body || {};
      if (!chatId || !newText || (!messageId && !clientMessageId)) {
        return res.status(400).json({ error: 'chatId, newText, and messageId are required' });
      }

      const record = await editMessageInDb({ chatId, messageId, clientMessageId, newText, senderId });
      if (!record) {
        return res.status(404).json({ error: 'Message not found' });
      }

      // Broadcast edit to room
      io.to(chatId).emit('message:edited', record);
      if (chatId.startsWith('group_')) {
        io.to(chatId).emit('group_message:edited', record);
      }

      res.json({ ok: true, message: record });
    } catch (err) {
      console.error('POST /messages/edit error:', err);
      res.status(500).json({ error: 'Unable to edit message' });
    }
  });

  // ── REST: react to message ──────────────────────────────────────────────────
  app.post('/messages/react', async (req, res) => {
    try {
      const { chatId, messageId, clientMessageId, userId, userName, emoji } = req.body || {};
      if (!chatId || !userId || !emoji || (!messageId && !clientMessageId)) {
        return res.status(400).json({ error: 'chatId, userId, emoji, and messageId are required' });
      }

      const record = await reactToMessageInDb({ chatId, messageId, clientMessageId, userId, userName, emoji });
      if (!record) {
        return res.status(404).json({ error: 'Message not found' });
      }

      // Broadcast reaction to room
      io.to(chatId).emit('message:reacted', record);
      if (chatId.startsWith('group_')) {
        io.to(chatId).emit('group_message:reacted', record);
      }

      res.json({ ok: true, message: record });
    } catch (err) {
      console.error('POST /messages/react error:', err);
      res.status(500).json({ error: 'Unable to react to message' });
    }
  });

  // ── REST: load group messages strictly ─────────────────────────────────────
  app.get('/group-messages', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || (req.query.groupId ? `group_${req.query.groupId}` : '')).trim();
      if (!chatId) return res.status(400).json({ error: 'chatId or groupId is required' });
      const limit = req.query.limit ? parseInt(req.query.limit, 10) : 100;
      const parsedLimit = Math.min(Math.max(limit, 1), 500);
      const msgs = await GroupMessage.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean();
      res.json(msgs.reverse().map(serializeRecord));
    } catch (err) {
      console.error('GET /group-messages error:', err);
      res.status(500).json({ error: 'Unable to load group messages' });
    }
  });

  // ── REST: get pending group invites for a user ─────────────────────────────
  app.get('/group-invites', async (req, res) => {
    try {
      const userId = String(req.query.userId || '').trim();
      if (!userId) return res.status(400).json({ error: 'userId is required' });

      // Find in messages collection, massage_notification collection, and group_notification collection
      const [inviteMsgs, massageNotifs, notifs] = await Promise.all([
        Message.find({
          receiverId: userId,
          text: { $regex: '^\\[GROUP_INVITE\\]:' }
        }).sort({ createdAt: -1 }).limit(50).lean(),
        MassageNotification.find({
          receiverId: userId,
          $or: [
            { type: { $in: ['invite', 'group_invite'] } },
            { text: { $regex: '^\\[GROUP_INVITE\\]:' } }
          ]
        }).sort({ createdAt: -1 }).limit(50).lean(),
        GroupNotification.find({
          receiverId: userId,
          type: 'invite'
        }).sort({ createdAt: -1 }).limit(50).lean(),
      ]);

      const invites = [];
      const seenGroupIds = new Set();

      // Parse from inviteMsgs and massageNotifs first (newest first)
      for (const m of [...inviteMsgs, ...massageNotifs]) {
        try {
          const inv = JSON.parse(m.text.replace('[GROUP_INVITE]:', ''));
          if (inv?.groupId && !seenGroupIds.has(inv.groupId)) {
            seenGroupIds.add(inv.groupId);
            invites.push({
              ...inv,
              status: inv.status || 'pending',
              messageId: String(m._id),
              clientMessageId: m.clientMessageId,
              timestamp: m.createdAt || inv.timestamp,
            });
          }
        } catch {}
      }

      // Also parse from notifs
      for (const n of notifs) {
        try {
          let inv = n.data;
          if (typeof inv === 'string') inv = JSON.parse(inv);
          if (inv?.groupId && !seenGroupIds.has(inv.groupId)) {
            seenGroupIds.add(inv.groupId);
            invites.push({
              ...inv,
              status: inv.status || 'pending',
              messageId: String(n._id),
              clientMessageId: n.clientMessageId,
              timestamp: n.createdAt || inv.timestamp,
            });
          }
        } catch {}
      }

      res.json(invites);
    } catch (err) {
      console.error('GET /group-invites error:', err);
      res.status(500).json({ error: 'Unable to load group invites' });
    }
  });

  // ── REST: load massage notifications strictly ──────────────────────────────
  app.get('/massage-notifications', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || (req.query.groupId ? `group_${req.query.groupId}` : '')).trim();
      const userId = String(req.query.userId || '').trim();
      const type = req.query.type ? String(req.query.type).trim() : null;
      const limit = req.query.limit ? parseInt(req.query.limit, 10) : 100;
      const parsedLimit = Math.min(Math.max(limit, 1), 500);

      const filter = {};
      if (chatId) filter.chatId = chatId;
      if (userId) filter.receiverId = userId;
      if (type) filter.type = type;

      const notifs = await MassageNotification.find(filter).sort({ createdAt: -1 }).limit(parsedLimit).lean();
      res.json(notifs.map(serializeRecord));
    } catch (err) {
      console.error('GET /massage-notifications error:', err);
      res.status(500).json({ error: 'Unable to load massage notifications' });
    }
  });

  // ── REST: load group notifications strictly ────────────────────────────────
  app.get('/group-notifications', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || (req.query.groupId ? `group_${req.query.groupId}` : '')).trim();
      if (!chatId) return res.status(400).json({ error: 'chatId or groupId is required' });
      const type = req.query.type ? String(req.query.type).trim() : null;
      const limit = req.query.limit ? parseInt(req.query.limit, 10) : 100;
      const parsedLimit = Math.min(Math.max(limit, 1), 500);
      const filter = { chatId };
      if (type) filter.type = type;
      const notifs = await GroupNotification.find(filter).sort({ createdAt: -1 }).limit(parsedLimit).lean();
      res.json(notifs.map(serializeRecord));
    } catch (err) {
      console.error('GET /group-notifications error:', err);
      res.status(500).json({ error: 'Unable to load group notifications' });
    }
  });

  // ── REST: rename a group ──────────────────────────────────────────────────
  app.post('/groups/rename', async (req, res) => {
    try {
      const { groupId, newName, userId, userName } = req.body || {};
      if (!groupId || !newName) {
        return res.status(400).json({ error: 'groupId and newName are required' });
      }
      const cleanName = String(newName).trim();
      const chatId = `group_${groupId}`;

      // 1. Update groupName in all messages for this group in group_massages
      await GroupMessage.updateMany(
        { $or: [{ groupId }, { chatId }] },
        { $set: { groupName: cleanName } }
      );

      // 2. Save a record in group_notification
      const notifDoc = new GroupNotification({
        chatId,
        groupId,
        type: 'name_updated',
        data: {
          groupId,
          groupName: cleanName,
          updatedBy: userId || 'unknown',
          updatedByName: userName || 'Admin',
        },
        createdAt: new Date(),
      });
      await notifDoc.save();

      // 3. Broadcast to socket room
      io.to(chatId).emit('group:updated', {
        groupId,
        groupName: cleanName,
        updatedBy: userId,
        updatedByName: userName,
      });

      console.log(`[group] Renamed group ${groupId} to "${cleanName}" by ${userName || userId}`);
      res.json({ ok: true, groupId, groupName: cleanName });
    } catch (err) {
      console.error('POST /groups/rename error:', err);
      res.status(500).json({ error: 'Failed to rename group' });
    }
  });

  // ── REST: delete messages for a chat ───────────────────────────────────────
  app.delete('/messages', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || '').trim();
      if (!chatId) return res.status(400).json({ error: 'chatId is required' });

      if (chatId.startsWith('group_')) {
        const [delMsgs, delNotifs, delMassageNotifs] = await Promise.all([
          GroupMessage.deleteMany({ chatId }),
          GroupNotification.deleteMany({ chatId }),
          MassageNotification.deleteMany({ chatId }),
        ]);
        const total = (delMsgs.deletedCount || 0) + (delNotifs.deletedCount || 0) + (delMassageNotifs.deletedCount || 0);
        console.log(`[group] Deleted ${total} records for chatId=${chatId}`);
        res.json({ ok: true, deletedCount: total });
      } else {
        const [delMsgs, delMassageNotifs] = await Promise.all([
          Message.deleteMany({ chatId }),
          MassageNotification.deleteMany({ chatId }),
        ]);
        const total = (delMsgs.deletedCount || 0) + (delMassageNotifs.deletedCount || 0);
        console.log(`[messages] Deleted ${total} messages/notifications for chatId=${chatId}`);
        res.json({ ok: true, deletedCount: total });
      }
    } catch (err) {
      console.error('DELETE /messages error:', err);
      res.status(500).json({ error: 'Unable to delete messages' });
    }
  });

  // ── REST: mark all messages in a chat as read for a reader ───────────────────
  // Called by ChatScreen immediately on open (REST, not socket-dependent).
  // This guarantees MongoDB has the correct readAt before FriendScreen re-fetches
  // unread counts via useFocusEffect when the user navigates back.
  app.post('/messages/mark-read', async (req, res) => {
    try {
      const { chatId, readerId } = req.body || {};
      if (!chatId || !readerId) {
        return res.status(400).json({ error: 'chatId and readerId are required' });
      }
      const readAt = new Date();
      await markConversationRead(chatId, readerId, readAt);
      console.log(`[mark-read] chatId=${chatId} readerId=${readerId}`);

      // Broadcast chat:read to everyone in the room!
      io.to(chatId).emit('chat:read', {
        chatId,
        readerId,
        readAt: readAt.toISOString(),
      });

      // Also forward to peer socket if peer is on another screen
      for (const [sid, meta] of socketMeta.entries()) {
        if (meta.userId !== readerId && meta.chatId !== chatId) {
          const sock = io.sockets.sockets.get(sid);
          if (sock) {
            sock.emit('chat:read', { chatId, readerId, readAt: readAt.toISOString() });
          }
        }
      }

      res.json({ ok: true });
    } catch (err) {
      console.error('POST /messages/mark-read error:', err);
      res.status(500).json({ error: 'Unable to mark messages as read' });
    }
  });

  // ── REST: online status query ────────────────────────────────────────────────
  app.get('/presence/:userId', (req, res) => {
    const userId   = req.params.userId;
    const presence = userPresence.get(userId);
    res.json({
      userId,
      isOnline:  presence?.isActive ?? false,
      lastSeen:  presence?.lastSeen ?? null,
    });
  });

  // ─── Socket.IO ───────────────────────────────────────────────────────────────
  io.on('connection', (socket) => {
    const queryUserId = String(socket.handshake.query.userId || '').trim();
    const queryChatId = String(socket.handshake.query.chatId || '').trim();
    const queryMode   = String(socket.handshake.query.mode   || 'chat').trim();

    // Auto-join the chat room from query params (for chat sockets)
    if (queryChatId) {
      socket.join(queryChatId);
    }

    // Store socket metadata
    socketMeta.set(socket.id, {
      userId: queryUserId,
      chatId: queryChatId || null,
      mode:   queryMode,
    });

    // Mark user online immediately on connection
    if (queryUserId) {
      setUserOnline(queryUserId, socket.id, queryChatId, null);
      console.log(`[connect] userId=${queryUserId} mode=${queryMode} chatId=${queryChatId || 'none'}`);
    }

    // ── user:register ──────────────────────────────────────────────────────────
    // Called by background socket (HomeScreen) to register for global notifications
    socket.on('user:register', ({ userId } = {}) => {
      if (!userId) return;
      const meta = socketMeta.get(socket.id) || {};
      socketMeta.set(socket.id, { ...meta, userId, mode: 'background' });
      setUserOnline(userId, socket.id, null, null);
      console.log(`[user:register] userId=${userId} background socket registered`);
    });

    // ── chat:join ─────────────────────────────────────────────────────────────
    // Client joins a specific chat room and announces they're online
    socket.on('chat:join', ({ chatId: roomId, userId, userName, otherUserId } = {}) => {
      if (!roomId) return;

      socket.join(roomId);

      // Update metadata
      const meta = socketMeta.get(socket.id) || {};
      socketMeta.set(socket.id, { ...meta, userId: userId || meta.userId, chatId: roomId });

      const uid  = userId || meta.userId;
      const name = userName || uid;

      if (uid) {
        setUserOnline(uid, socket.id, roomId, name);
      }

      // Broadcast join to everyone else in the room
      socket.to(roomId).emit('chat:join', {
        chatId: roomId,
        userId: uid,
        userName: name,
      });

      // Also broadcast their online presence status to the room
      if (uid) {
        socket.to(roomId).emit('presence:status', {
          chatId:   roomId,
          userId:   uid,
          userName: name,
          isActive: true,
        });
      }

      // Reply to the joining user with the presence of the other person
      // (so they immediately see "Online" if the peer is already in the room)
      if (otherUserId) {
        const peerOnline = isUserOnline(otherUserId);
        socket.emit('presence:status', {
          chatId:   roomId,
          userId:   otherUserId,
          isActive: peerOnline,
        });
        console.log(`[chat:join] Replying to ${uid}: peer ${otherUserId} isActive=${peerOnline}`);

        // When joining a 1-to-1 chat, mark past messages as read immediately and notify peer
        if (!roomId.startsWith('group_')) {
          const readAt = new Date();
          markConversationRead(roomId, uid, readAt).then(() => {
            io.to(roomId).emit('chat:read', {
              chatId: roomId,
              readerId: uid,
              readAt: readAt.toISOString(),
            });
          }).catch(() => {});
        }
      }
    });

    // ── presence:status ───────────────────────────────────────────────────────
    // Client broadcasting their own active/inactive state — relay to the room
    socket.on('presence:status', ({ chatId: roomId, userId, userName, isActive } = {}) => {
      if (!roomId || !userId) return;

      if (isActive) {
        setUserOnline(userId, socket.id, roomId, userName);
      } else {
        setUserOffline(userId);
      }

      // Relay to everyone else in the room
      socket.to(roomId).emit('presence:status', {
        chatId:   roomId,
        userId,
        userName: userName || userId,
        isActive: Boolean(isActive),
      });
    });

    // ── presence:request ──────────────────────────────────────────────────────
    // Client asking for the peer's current presence — server replies directly
    socket.on('presence:request', ({ chatId: roomId, requesterId, targetUserId } = {}) => {
      if (!targetUserId) return;

      const peerOnline = isUserOnline(targetUserId);
      // Reply only to the requesting socket
      socket.emit('presence:status', {
        chatId:   roomId,
        userId:   targetUserId,
        isActive: peerOnline,
      });
      console.log(`[presence:request] ${requesterId} asked about ${targetUserId}: isActive=${peerOnline}`);
    });

    // ── presence:ping / presence:pong ─────────────────────────────────────────
    socket.on('presence:ping', ({ chatId: roomId, userId } = {}) => {
      if (!roomId || !userId) return;
      socket.to(roomId).emit('presence:ping', { chatId: roomId, userId });
    });

    socket.on('presence:pong', ({ chatId: roomId, userId } = {}) => {
      if (!roomId || !userId) return;
      socket.to(roomId).emit('presence:pong', { chatId: roomId, userId });
    });

    // ── typing:status ─────────────────────────────────────────────────────────
    socket.on('typing:status', ({ chatId: roomId, userId, userName, isTyping } = {}) => {
      if (!roomId || !userId) return;
      socket.to(roomId).emit('typing:status', {
        chatId:   roomId,
        userId,
        userName: userName || 'Someone',
        isTyping: Boolean(isTyping),
      });
    });

    // ── chat:read ─────────────────────────────────────────────────────────────
    socket.on('chat:read', async ({ chatId: roomId, readerId, readerName } = {}) => {
      try {
        if (!roomId || !readerId) return;
        const readAt = new Date();
        await markConversationRead(roomId, readerId, readAt);
        io.to(roomId).emit('chat:read', {
          chatId:     roomId,
          readerId,
          readerName: readerName || 'Reader',
          readAt:     readAt.toISOString(),
        });
        // Also forward to background sockets of the peer
        for (const [sid, meta] of socketMeta.entries()) {
          if (meta.userId !== readerId && meta.chatId !== roomId) {
            const sock = io.sockets.sockets.get(sid);
            if (sock) {
              sock.emit('chat:read', { chatId: roomId, readerId, readAt: readAt.toISOString() });
            }
          }
        }
      } catch (err) {
        console.error('chat:read error:', err);
      }
    });

    // ── chat:leave ────────────────────────────────────────────────────────────
    socket.on('chat:leave', ({ chatId: roomId, userId } = {}) => {
      if (!roomId) return;
      const uid = userId || socketMeta.get(socket.id)?.userId;
      socket.leave(roomId);
      if (uid) {
        setUserOffline(uid);
        socket.to(roomId).emit('chat:leave', { chatId: roomId, userId: uid });
        socket.to(roomId).emit('presence:status', {
          chatId:   roomId,
          userId:   uid,
          isActive: false,
        });
        console.log(`[chat:leave] userId=${uid} left room=${roomId}`);
      }
    });

    // ── message:send ──────────────────────────────────────────────────────────
    socket.on('message:send', async (payload, ack) => {
      try {
        const { chatId: roomId, senderId, senderName, receiverId, text, clientMessageId } = payload || {};
        if (!roomId || !senderId || !text) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Missing required message fields' });
          return;
        }

        const { record, category } = await saveIncomingPacket(payload);

        // Deliver to everyone in the chat room
        io.to(roomId).emit('message:new', record);
        if (category === 'group_message') {
          io.to(roomId).emit('group_message:new', record);
        } else if (category === 'group_notification') {
          io.to(roomId).emit('group_notification:new', record);
        }

        // ── In-app toast: find receiver's socket and emit directly on ANY page ──
        if (receiverId && receiverId !== 'all') {
          const sentUserIds = new Set();
          for (const [sid, meta] of socketMeta.entries()) {
            if (meta.userId === receiverId && meta.chatId !== roomId && !sentUserIds.has(meta.userId)) {
              const bgSock = io.sockets.sockets.get(sid);
              if (bgSock) {
                bgSock.emit('message:new', record);
                sentUserIds.add(meta.userId);
                console.log(`[toast] Sent message:new to receiver socket of userId=${receiverId} (mode=${meta.mode})`);
              }
            }
          }
        }

        // ── Send FCM push notification (mobile system tray) via firebase-admin ─────
        if (category === 'message' && receiverId) {
          const receiverMeta = userPresence.get(receiverId);
          const receiverInRoom = receiverMeta?.chatId === roomId && receiverMeta?.isActive;
          if (!receiverInRoom) {
            const fcmToken = await getPushToken(receiverId);
            await sendFCMNotification(
              fcmToken,
              senderName || 'New message',
              text.length > 100 ? text.slice(0, 97) + '…' : text,
              { otherUserId: senderId, senderName: senderName || 'Someone', chatId: roomId }
            );
          }
        }

        if (typeof ack === 'function') ack({ ok: true, message: record });
      } catch (err) {
        console.error('message:send error:', err);
        if (typeof ack === 'function') ack({ ok: false, error: 'Unable to save message' });
      }
    });

    // ── group_message:send ───────────────────────────────────────────────────
    socket.on('group_message:send', async (payload, ack) => {
      try {
        const { chatId: roomId, senderId, text } = payload || {};
        if (!roomId || !senderId || !text) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Missing required group message fields' });
          return;
        }
        const { record } = await saveIncomingPacket({ ...payload, chatId: roomId });
        io.to(roomId).emit('group_message:new', record);
        io.to(roomId).emit('message:new', record);
        if (typeof ack === 'function') ack({ ok: true, message: record });
      } catch (err) {
        console.error('group_message:send error:', err);
        if (typeof ack === 'function') ack({ ok: false, error: 'Unable to save group message' });
      }
    });

    // ── group_notification:send ──────────────────────────────────────────────
    socket.on('group_notification:send', async (payload, ack) => {
      try {
        const { chatId: roomId, text } = payload || {};
        if (!roomId || !text) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Missing required notification fields' });
          return;
        }
        const { record } = await saveIncomingPacket({ ...payload, chatId: roomId, isNotification: true });
        io.to(roomId).emit('group_notification:new', record);
        io.to(roomId).emit('message:new', record);
        if (typeof ack === 'function') ack({ ok: true, message: record });
      } catch (err) {
        console.error('group_notification:send error:', err);
        if (typeof ack === 'function') ack({ ok: false, error: 'Unable to save group notification' });
      }
    });

    // ── message:edit ──────────────────────────────────────────────────────────
    socket.on('message:edit', async (payload, ack) => {
      try {
        const { chatId: roomId, messageId, clientMessageId, newText, senderId } = payload || {};
        if (!roomId || !newText || (!messageId && !clientMessageId)) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Missing required edit fields' });
          return;
        }

        const record = await editMessageInDb({ chatId: roomId, messageId, clientMessageId, newText, senderId });
        if (!record) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Message not found' });
          return;
        }

        io.to(roomId).emit('message:edited', record);
        if (roomId.startsWith('group_')) {
          io.to(roomId).emit('group_message:edited', record);
        }

        if (typeof ack === 'function') ack({ ok: true, message: record });
      } catch (err) {
        console.error('message:edit error:', err);
        if (typeof ack === 'function') ack({ ok: false, error: 'Unable to edit message' });
      }
    });

    // ── message:react ─────────────────────────────────────────────────────────
    socket.on('message:react', async (payload, ack) => {
      try {
        const { chatId: roomId, messageId, clientMessageId, userId, userName, emoji } = payload || {};
        if (!roomId || !userId || !emoji || (!messageId && !clientMessageId)) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Missing required reaction fields' });
          return;
        }

        const record = await reactToMessageInDb({ chatId: roomId, messageId, clientMessageId, userId, userName, emoji });
        if (!record) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Message not found' });
          return;
        }

        io.to(roomId).emit('message:reacted', record);
        if (roomId.startsWith('group_')) {
          io.to(roomId).emit('group_message:reacted', record);
        }

        if (typeof ack === 'function') ack({ ok: true, message: record });
      } catch (err) {
        console.error('message:react error:', err);
        if (typeof ack === 'function') ack({ ok: false, error: 'Unable to react to message' });
      }
    });

    // ── group:rename ──────────────────────────────────────────────────────────
    socket.on('group:rename', async (payload, ack) => {
      try {
        const { groupId, newName, userId, userName } = payload || {};
        if (!groupId || !newName) {
          if (typeof ack === 'function') ack({ ok: false, error: 'Missing groupId or newName' });
          return;
        }
        const cleanName = String(newName).trim();
        const chatId = `group_${groupId}`;

        await GroupMessage.updateMany(
          { $or: [{ groupId }, { chatId }] },
          { $set: { groupName: cleanName } }
        );

        const notifDoc = new GroupNotification({
          chatId,
          groupId,
          type: 'name_updated',
          data: {
            groupId,
            groupName: cleanName,
            updatedBy: userId || 'unknown',
            updatedByName: userName || 'Admin',
          },
          createdAt: new Date(),
        });
        await notifDoc.save();

        io.to(chatId).emit('group:updated', {
          groupId,
          groupName: cleanName,
          updatedBy: userId,
          updatedByName: userName,
        });

        console.log(`[group] Socket renamed group ${groupId} to "${cleanName}"`);
        if (typeof ack === 'function') ack({ ok: true, groupId, groupName: cleanName });
      } catch (err) {
        console.error('group:rename error:', err);
        if (typeof ack === 'function') ack({ ok: false, error: 'Failed to rename group' });
      }
    });

    // ── disconnect ────────────────────────────────────────────────────────────
    socket.on('disconnect', (reason) => {
      const meta = socketMeta.get(socket.id);
      const uid  = meta?.userId;
      const room = meta?.chatId;

      socketMeta.delete(socket.id);

      if (!uid) return;

      // Mark offline in presence store
      setUserOffline(uid);

      // Notify the chat room that this user left
      if (room) {
        io.to(room).emit('chat:leave', { chatId: room, userId: uid });
        io.to(room).emit('presence:status', {
          chatId:   room,
          userId:   uid,
          isActive: false,
        });
      }

      console.log(`[disconnect] userId=${uid} reason=${reason} room=${room || 'none'}`);
    });
  });

  server.listen(PORT, () => {
    console.log(`Chat server listening on http://localhost:${PORT}`);
  });
}

main().catch((err) => {
  console.error('Chat server failed to start:', err);
  process.exit(1);
});
