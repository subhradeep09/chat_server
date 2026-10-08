const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const mongoose = require('mongoose');
const { Server } = require('socket.io');
const admin = require('firebase-admin');

dotenv.config();
if (!process.env.MONGODB_URI) {
  dotenv.config({ path: path.resolve(__dirname, '..', '.env') });
}

const PORT         = process.env.PORT || process.env.CHAT_SERVER_PORT || 3001;
const MONGODB_URI  = process.env.MONGODB_URI;
const MONGODB_DB   = process.env.MONGODB_DB || 'chat_puja';
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

// ─── IST (Indian Standard Time, UTC+5:30) Helpers ───────────────────────────
const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;

function getISTDate(date) {
  if (date instanceof Date && !isNaN(date.getTime())) {
    return date;
  }
  if (typeof date === 'string' || typeof date === 'number') {
    const parsed = new Date(date);
    if (!isNaN(parsed.getTime())) return parsed;
  }
  return new Date(Date.now() + IST_OFFSET_MS);
}

function getISTString(date) {
  if (typeof date === 'string') {
    if (date.includes('+05:30')) return date;
    if (date.endsWith('Z')) return date.replace('Z', '+05:30');
  }
  if (date instanceof Date && !isNaN(date.getTime())) {
    return date.toISOString().replace('Z', '+05:30');
  }
  const ist = new Date(Date.now() + IST_OFFSET_MS);
  return ist.toISOString().replace('Z', '+05:30');
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
    deliveredAt:     { type: Date, default: getISTDate },
    createdAt:       { type: Date, default: getISTDate },
    istTime:         { type: String, default: getISTString },
    readAt:          { type: Date, default: null },
    replyTo:         { type: mongoose.Schema.Types.Mixed, default: null },
    isEdited:        { type: Boolean, default: false },
    editedAt:        { type: Date, default: null },
    reactions:       { type: Array, default: [] },
  },
  { timestamps: false, versionKey: false, collection: 'messages' }
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
    deliveredAt:     { type: Date, default: getISTDate },
    createdAt:       { type: Date, default: getISTDate },
    istTime:         { type: String, default: getISTString },
    readAt:          { type: Date, default: null },
    replyTo:         { type: mongoose.Schema.Types.Mixed, default: null },
    isEdited:        { type: Boolean, default: false },
    editedAt:        { type: Date, default: null },
    reactions:       { type: Array, default: [] },
  },
  { timestamps: false, versionKey: false, collection: 'group_massages' }
);
groupMessageSchema.index({ chatId: 1, createdAt: 1 });
groupMessageSchema.index({ groupId: 1, createdAt: -1 });
const GroupMessage = mongoose.model('GroupMessage', groupMessageSchema);

// ── 3. group_notification collection (Group Notifications & System Events) ──
const groupNotificationSchema = new mongoose.Schema(
  {
    chatId:          { type: String, required: true, index: true },
    groupId:         { type: String, required: true, index: true },
    groupName:       { type: String, default: '' },
    type:            { type: String, required: true, index: true }, // 'system' | 'invite' | 'invite_accepted' | 'invite_rejected' | 'member_sync' | 'member_left' | 'member_kicked' | 'location' | 'location_stopped' | 'name_updated'
    senderId:        { type: String, default: null },
    senderName:      { type: String, default: null },
    receiverId:      { type: String, default: 'all' },
    text:            { type: String, required: true },
    data:            { type: mongoose.Schema.Types.Mixed, default: null },
    clientMessageId: { type: String, default: null, index: true },
    deliveredAt:     { type: Date, default: getISTDate },
    createdAt:       { type: Date, default: getISTDate },
    istTime:         { type: String, default: getISTString },
  },
  { timestamps: false, versionKey: false, collection: 'group_notification' }
);
groupNotificationSchema.index({ chatId: 1, createdAt: -1 });
groupNotificationSchema.index({ groupId: 1, type: 1 });
groupNotificationSchema.index({ type: 1, 'data.targetUserId': 1 });
groupNotificationSchema.index({ type: 1, 'data.kickedUserId': 1 });
// TTL index: auto-delete ephemeral group live location coordinate updates after 24 hours (86400s)
groupNotificationSchema.index(
  { createdAt: 1 },
  { expireAfterSeconds: 86400, partialFilterExpression: { type: { $in: ['location', 'location_stopped'] } } }
);
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
    deliveredAt:     { type: Date, default: getISTDate },
    createdAt:       { type: Date, default: getISTDate },
    istTime:         { type: String, default: getISTString },
    readAt:          { type: Date, default: null },
  },
  { timestamps: false, versionKey: false, collection: 'massage_notification' }
);
massageNotificationSchema.index({ chatId: 1, createdAt: -1 });
massageNotificationSchema.index({ receiverId: 1, type: 1 });
massageNotificationSchema.index({ groupId: 1, createdAt: -1 });
// TTL index: auto-delete ephemeral 1-on-1 live location coordinate updates after 24 hours (86400s)
massageNotificationSchema.index(
  { createdAt: 1 },
  { expireAfterSeconds: 86400, partialFilterExpression: { type: { $in: ['live_location', 'live_location_stopped', 'location_stopped'] } } }
);
const MassageNotification = mongoose.model('MassageNotification', massageNotificationSchema);

// ── 4. notifytoken collection ──
// Stores Expo push tokens permanently in MongoDB (collection: notifytoken).
// One document per user — upserted on every app login.
// Survives server restarts unlike the old in-memory Map.
const notifyTokenSchema = new mongoose.Schema(
  {
    userId:    { type: String, required: true, unique: true, index: true },
    token:     { type: String, required: true },
    createdAt: { type: Date, default: getISTDate },
    updatedAt: { type: Date, default: getISTDate },
    istTime:   { type: String, default: getISTString },
  },
  { timestamps: false, versionKey: false, collection: 'notifytoken' }
);
const NotifyToken = mongoose.model('NotifyToken', notifyTokenSchema);

// ── notifytoken helpers ──

/** Save (upsert) a push token for a user — called when app registers */
async function savePushToken(userId, token) {
  await NotifyToken.findOneAndUpdate(
    { userId },
    {
      $set: { userId, token, updatedAt: getISTDate(), istTime: getISTString() },
      $setOnInsert: { createdAt: getISTDate() },
    },
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



// ── 6. groups collection (Group Metadata & Membership) ──
const groupMemberSubSchema = new mongoose.Schema(
  {
    id:        { type: String, required: true },
    name:      { type: String, default: '' },
    avatar_id: { type: mongoose.Schema.Types.Mixed, default: '1' },
    photo:     { type: String, default: null },
    role:      { type: String, enum: ['admin', 'member'], default: 'member' },
    joinedAt:  { type: String, default: getISTString },
  },
  { _id: false }
);

const groupSchema = new mongoose.Schema(
  {
    id:              { type: String, required: true, unique: true, index: true },
    name:            { type: String, required: true, trim: true },
    description:     { type: String, default: '' },
    createdBy:       { type: String, required: true, index: true },
    createdByName:   { type: String, default: '' },
    createdAt:       { type: String, default: getISTString },
    createdAtDate:   { type: Date, default: getISTDate },
    updatedAtDate:   { type: Date, default: getISTDate },
    istTime:         { type: String, default: getISTString },
    members:         { type: [groupMemberSubSchema], default: [] },
    icon:            { type: String, default: 'friends' },
    lastMessage:     { type: String, default: 'Group created' },
    lastMessageTime: { type: String, default: getISTString },
    isDeleted:       { type: Boolean, default: false, index: true },
  },
  { timestamps: false, versionKey: false, collection: 'groups' }
);
groupSchema.index({ 'members.id': 1, isDeleted: 1 });
groupSchema.index({ createdAt: -1 });
const Group = mongoose.model('Group', groupSchema);

// ── 7. active_live_locations collection (State-Based Live Location for Personal & Group chats) ──
const activeLiveLocationSchema = new mongoose.Schema(
  {
    chatId:     { type: String, required: true, index: true },
    userId:     { type: String, required: true, index: true },
    userName:   { type: String, default: '' },
    avatarId:   { type: String, default: null },
    photo:      { type: String, default: null },
    latitude:   { type: Number, required: true },
    longitude:  { type: Number, required: true },
    isGroup:    { type: Boolean, default: false },
    groupId:    { type: String, default: null },
    groupName:  { type: String, default: '' },
    receiverId: { type: String, default: null },
    isActive:   { type: Boolean, default: true },
    updatedAt:  { type: Date, default: getISTDate },
    createdAt:  { type: Date, default: getISTDate },
    istTime:    { type: String, default: getISTString },
  },
  { versionKey: false, collection: 'active_live_locations' }
);
activeLiveLocationSchema.index({ chatId: 1, userId: 1 }, { unique: true });
activeLiveLocationSchema.index({ chatId: 1, isActive: 1 });
activeLiveLocationSchema.index({ updatedAt: 1 }, { expireAfterSeconds: 86400 });
const ActiveLiveLocation = mongoose.model('ActiveLiveLocation', activeLiveLocationSchema);

function serializeGroup(g) {
  if (!g) return null;
  return {
    id:              g.id,
    name:            g.name,
    description:     g.description || '',
    createdBy:       g.createdBy,
    createdByName:   g.createdByName || '',
    createdAt:       g.createdAt || (g.createdAtDate ? g.createdAtDate.toISOString() : new Date().toISOString()),
    members:         Array.isArray(g.members) ? g.members : [],
    icon:            g.icon || 'friends',
    lastMessage:     g.lastMessage || 'Group created',
    lastMessageTime: g.lastMessageTime || g.createdAt || new Date().toISOString(),
  };
}

// ── Helpers for packet classification & serialization ──

function isGroupNotificationPacket(text, isNotificationFlag = false) {
  if (isNotificationFlag) return true;
  if (!text || typeof text !== 'string') return false;
  return (
    text.startsWith('[GROUP_LOCATION]:') ||
    text.startsWith('[GROUP_LOCATION_STOPPED]:') ||
    text.startsWith('[GROUP_MEMBER_SYNC]:') ||
    text.startsWith('[GROUP_MEMBER_LEFT]:') ||
    text.startsWith('[GROUP_MEMBER_KICKED]:') ||
    text.startsWith('[GROUP_NAME_UPDATED]:') ||
    text.startsWith('[GROUP_SYSTEM]:') ||
    text.startsWith('[GROUP_')
  );
}

function isPersonalNotificationPacket(text, isNotificationFlag = false) {
  if (isNotificationFlag) return true;
  if (!text || typeof text !== 'string') return false;
  return (
    text.startsWith('[LIVE_LOCATION]:') ||
    text.startsWith('[LIVE_LOCATION_STOPPED]:') ||
    text.startsWith('[LOCATION_STOPPED]:') ||
    text.startsWith('[GROUP_INVITE]:') ||
    text.startsWith('[GROUP_INVITE_ACCEPTED]:') ||
    text.startsWith('[GROUP_INVITE_REJECTED]:')
  );
}

function parseGroupNotification(text, chatId, payload = {}) {
  let type = 'general';
  let data = null;
  let groupId = (chatId && chatId.startsWith('group_')) ? chatId.replace(/^group_/, '') : (payload.groupId || null);
  let groupName = payload.groupName || payload.group_name || '';

  if (typeof text === 'string') {
    const match = text.match(/^\[(GROUP_[A-Z_]+)\]:(.*)$/s);
    if (match) {
      const rawTag = match[1];
      const payloadStr = match[2];
      type = rawTag.replace(/^GROUP_/, '').toLowerCase();
      try {
        data = JSON.parse(payloadStr);
      } catch {
        data = { raw: payloadStr };
      }
      if (data && typeof data === 'object') {
        if (data.groupId) groupId = data.groupId;
        if (data.groupName) groupName = data.groupName;
      }
    } else {
      const genericMatch = text.match(/^\[([A-Z_]+)\]:(.*)$/s);
      if (genericMatch) {
        type = genericMatch[1].toLowerCase();
        try {
          data = JSON.parse(genericMatch[2]);
        } catch {
          data = { raw: genericMatch[2] };
        }
        if (data && typeof data === 'object') {
          if (data.groupId) groupId = data.groupId;
          if (data.groupName) groupName = data.groupName;
        }
      }
    }
  }

  return { type, data, groupId, groupName };
}

function parseMassageNotification(text, chatId, payload = {}) {
  let type = 'general';
  let data = null;
  let groupId = payload.groupId || null;
  let groupName = payload.groupName || payload.group_name || '';

  if (typeof text === 'string') {
    const match = text.match(/^\[([A-Z_]+)\]:(.*)$/s);
    if (match) {
      const rawTag = match[1];
      const payloadStr = match[2];
      if (rawTag === 'LIVE_LOCATION') {
        type = 'live_location';
      } else if (rawTag.includes('LOCATION_STOPPED')) {
        type = 'live_location_stopped';
      } else if (rawTag === 'GROUP_INVITE') {
        type = 'invite';
      } else if (rawTag === 'GROUP_INVITE_ACCEPTED') {
        type = 'invite_accepted';
      } else if (rawTag === 'GROUP_INVITE_REJECTED') {
        type = 'invite_rejected';
      } else {
        type = rawTag.toLowerCase();
      }
      try {
        data = JSON.parse(payloadStr);
      } catch {
        data = { raw: payloadStr };
      }
      if (data && typeof data === 'object') {
        if (data.groupId) groupId = data.groupId;
        if (data.groupName) groupName = data.groupName;
      }
    }
  }

  return { type, data, groupId, groupName };
}

function serializeRecord(doc) {
  const istTimeStr = doc.istTime || getISTString(doc.createdAt);
  const deliveredStr = doc.deliveredAt ? getISTString(doc.deliveredAt) : istTimeStr;
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
    editedAt:        doc.editedAt ? getISTString(doc.editedAt) : null,
    reactions:       Array.isArray(doc.reactions) ? doc.reactions : [],
    createdAt:       istTimeStr,
    deliveredAt:     deliveredStr,
    istTime:         istTimeStr,
    readAt:          doc.readAt ? getISTString(doc.readAt) : null,
  };
}

async function saveIncomingPacket(payload) {
  const { chatId, senderId, senderName, receiverId, text, clientMessageId, replyTo } = payload;
  const isGroup = Boolean(chatId && chatId.startsWith('group_'));

  // ── 1. GROUP NOTIFICATION -> group_notification collection ──
  // All group events: member join, group live location, rename, kick, left, system
  if (isGroup && isGroupNotificationPacket(text, payload.isNotification)) {
    const { type, data, groupId, groupName } = parseGroupNotification(text, chatId, payload);

    // Live location tracking in ActiveLiveLocation
    if (type === 'location' && data && data.userId && data.latitude != null && data.longitude != null) {
      ActiveLiveLocation.findOneAndUpdate(
        { chatId, userId: data.userId },
        {
          $set: {
            chatId,
            userId: data.userId,
            userName: data.userName || senderName || '',
            avatarId: data.avatarId || null,
            photo: data.photo || null,
            latitude: Number(data.latitude),
            longitude: Number(data.longitude),
            isGroup: true,
            groupId: groupId || chatId.replace(/^group_/, ''),
            groupName: groupName || '',
            isActive: true,
            updatedAt: getISTDate(),
            istTime: getISTString(),
          },
          $setOnInsert: {
            createdAt: getISTDate(),
          },
        },
        { upsert: true, new: true }
      ).catch((e) => console.warn('[location] Error upserting group live location:', e));
    } else if (type === 'location_stopped' && (data?.userId || senderId)) {
      const stopUid = data?.userId || senderId;
      ActiveLiveLocation.deleteMany({ chatId, userId: stopUid })
        .catch((e) => console.warn('[location] Error deleting group live location:', e));
    }

    const doc = await GroupNotification.create({
      chatId,
      groupId: groupId || chatId.replace(/^group_/, ''),
      groupName: groupName || '',
      type,
      senderId: senderId || null,
      senderName: senderName || 'System',
      receiverId: receiverId || 'all',
      text,
      data,
      clientMessageId: clientMessageId || null,
      deliveredAt: getISTDate(),
      createdAt: getISTDate(),
      istTime: getISTString(),
    });

    // If a member was kicked, purge any old group invite messages for that user & group and remove from Group.members
    if (type === 'member_kicked') {
      const targetUserId = data?.targetUserId || data?.kickedUserId;
      const kickGroupId = groupId || data?.groupId;
      if (targetUserId && kickGroupId) {
        Group.updateOne({ id: kickGroupId }, { $pull: { members: { id: targetUserId } } }).catch(() => {});
        try {
          const inviteRegex = new RegExp(`\\[GROUP_INVITE\\]:.*"groupId":"${kickGroupId}"`);
          await Promise.all([
            MassageNotification.deleteMany({
              $or: [
                { receiverId: targetUserId, groupId: kickGroupId },
                { receiverId: targetUserId, text: { $regex: inviteRegex } },
                { type: 'invite', groupId: kickGroupId, receiverId: targetUserId },
                { type: 'group_invite', groupId: kickGroupId, receiverId: targetUserId }
              ]
            }),
            Message.deleteMany({
              $or: [{ receiverId: targetUserId }, { senderId: targetUserId }],
              text: { $regex: inviteRegex }
            }),
            GroupNotification.deleteMany({
              groupId: kickGroupId,
              receiverId: targetUserId,
              type: 'invite'
            })
          ]);
          console.log(`[member_kicked] Purged old group invites for userId=${targetUserId} in groupId=${kickGroupId}`);
        } catch (e) {
          console.warn('[member_kicked] Failed to purge old invites:', e);
        }
      }
    }

    if (type === 'member_left') {
      const leftUserId = data?.userId || senderId;
      const leftGroupId = groupId || data?.groupId;
      if (leftGroupId && leftUserId) {
        Group.updateOne({ id: leftGroupId }, { $pull: { members: { id: leftUserId } } }).catch(() => {});
      }
    }

    if (type === 'member_sync') {
      const syncMember = data?.member;
      const syncGroupId = groupId || data?.groupId;
      if (syncGroupId && syncMember?.id) {
        Group.findOneAndUpdate(
          { id: syncGroupId, 'members.id': syncMember.id },
          { $set: { 'members.$': syncMember } }
        ).then((found) => {
          if (!found) {
            return Group.updateOne(
              { id: syncGroupId },
              { $push: { members: syncMember } }
            );
          }
        }).catch(() => {});
      }
    }

    if (type === 'name_updated') {
      const updatedName = data?.groupName || data?.newName;
      const updateGid = groupId || data?.groupId;
      if (updateGid && updatedName) {
        Group.updateOne({ id: updateGid }, { $set: { name: updatedName } }).catch(() => {});
      }
    }

    return { record: serializeRecord(doc), category: 'group_notification' };
  }

  // ── 2. PERSONAL NOTIFICATION -> massage_notification collection ──
  // 1-on-1 notifications: live location share, live location stop, group invites / status in direct messages
  if (!isGroup && isPersonalNotificationPacket(text, payload.isNotification)) {
    const { type, data, groupId, groupName } = parseMassageNotification(text, chatId, payload);

    // Live location tracking in ActiveLiveLocation
    if (type === 'live_location' && data && (data.latitude != null || data.lat != null)) {
      const lat = Number(data.latitude != null ? data.latitude : data.lat);
      const lng = Number(data.longitude != null ? data.longitude : data.lng);
      const uid = data.userId || senderId;
      ActiveLiveLocation.findOneAndUpdate(
        { chatId, userId: uid },
        {
          $set: {
            chatId,
            userId: uid,
            userName: data.userName || senderName || '',
            avatarId: data.avatarId || null,
            photo: data.photo || null,
            latitude: lat,
            longitude: lng,
            isGroup: false,
            receiverId: receiverId || null,
            isActive: true,
            updatedAt: getISTDate(),
            istTime: getISTString(),
          },
          $setOnInsert: {
            createdAt: getISTDate(),
          },
        },
        { upsert: true, new: true }
      ).catch((e) => console.warn('[location] Error upserting personal live location:', e));
    } else if (type === 'live_location_stopped') {
      const stopUid = data?.userId || data?.senderId || senderId;
      if (stopUid) {
        ActiveLiveLocation.deleteMany({ chatId, userId: stopUid })
          .catch((e) => console.warn('[location] Error deleting personal live location:', e));
      }
    }

    const doc = await MassageNotification.create({
      chatId,
      type,
      senderId: senderId || null,
      senderName: senderName || 'Unknown',
      receiverId: receiverId || null,
      groupId: groupId || null,
      groupName: groupName || '',
      text,
      data,
      clientMessageId: clientMessageId || null,
      deliveredAt: getISTDate(),
      createdAt: getISTDate(),
      istTime: getISTString(),
    });

    return { record: serializeRecord(doc), category: 'massage_notification' };
  }

  // ── 3. GROUP CHAT MESSAGE -> group_massages collection ──
  // Pure group text messages ONLY
  if (isGroup) {
    const groupId = chatId.replace(/^group_/, '');
    let groupName = payload.groupName || payload.group_name || '';

    // If groupName was not provided in the payload, look it up from group_notification
    if (!groupName) {
      try {
        const knownNotif = await GroupNotification.findOne({
          groupId,
          groupName: { $exists: true, $ne: '' }
        }).lean();
        if (knownNotif?.groupName) {
          groupName = knownNotif.groupName;
        } else if (knownNotif?.data?.groupName) {
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
      deliveredAt: getISTDate(),
      createdAt: getISTDate(),
      istTime: getISTString(),
    });

    // Update lastMessage and lastMessageTime on Group collection
    const cleanPreview = text.startsWith('[GROUP_SYSTEM]:')
      ? text.replace('[GROUP_SYSTEM]:', '')
      : `${senderName || 'Member'}: ${text}`;
    Group.updateOne(
      { id: groupId },
      { $set: { lastMessage: cleanPreview, lastMessageTime: getISTString() } }
    ).catch(() => {});

    return { record: serializeRecord(doc), category: 'group_message' };
  }

  // ── 4. PERSONAL CHAT MESSAGE -> messages collection ──
  // Pure 1-on-1 personal text messages ONLY
  const doc = await Message.create({
    chatId,
    senderId,
    senderName: senderName || 'Unknown',
    receiverId,
    text,
    clientMessageId: clientMessageId || null,
    replyTo: replyTo || null,
    deliveredAt: getISTDate(),
    createdAt: getISTDate(),
    istTime: getISTString(),
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
  doc.editedAt = getISTDate();
  doc.istTime = getISTString();
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

async function loadMessages(chatId, limit = 100, currentUserId = null) {
  const parsedLimit = Math.min(Math.max(parseInt(limit, 10) || 100, 1), 500);
  if (chatId.startsWith('group_')) {
    // Return group chat messages and group notifications ONLY
    const [groupMsgs, groupNotifs] = await Promise.all([
      GroupMessage.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
      GroupNotification.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
    ]);
    const seen = new Set();
    const combined = [];
    for (const item of [...groupMsgs, ...groupNotifs]) {
      const key = item.clientMessageId || String(item._id);
      if (!seen.has(key)) {
        seen.add(key);
        combined.push(item);
      }
    }
    combined.sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
    return combined.slice(-parsedLimit).map(serializeRecord);
  } else {
    // 1-on-1 direct messages: Determine the participant user IDs for this chat
    const participants = new Set();
    if (currentUserId) participants.add(currentUserId);
    if (chatId && chatId.includes('_')) {
      const parts = chatId.split('_');
      parts.forEach((p) => { if (p) participants.add(p); });
    }
    const candidateUserIds = Array.from(participants).filter(Boolean);

    // Build targeted query strictly for the participants of this conversation
    const kickFilter = { type: 'member_kicked' };
    if (candidateUserIds.length > 0) {
      kickFilter.$or = [
        { 'data.targetUserId': { $in: candidateUserIds } },
        { 'data.kickedUserId': { $in: candidateUserIds } },
        { receiverId: { $in: candidateUserIds } },
      ];
    }

    // Personal 1-on-1 direct messages (Message and MassageNotification ONLY)
    const [msgs, massageNotifs, kickedNotifs] = await Promise.all([
      Message.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
      MassageNotification.find({ chatId }).sort({ createdAt: -1 }).limit(parsedLimit).lean(),
      GroupNotification.find(kickFilter).sort({ createdAt: -1 }).limit(50).lean(),
    ]);

    const kickedPairs = new Set();
    for (const kn of kickedNotifs) {
      const gid = kn.groupId || kn.data?.groupId;
      const uid = kn.data?.targetUserId || kn.data?.kickedUserId;
      if (gid && uid) kickedPairs.add(`${gid}_${uid}`);
    }

    const seen = new Set();
    const seenGroupInvites = new Set();
    const combined = [];
    for (const item of [...msgs, ...massageNotifs]) {
      // Deduplicate group invite cards by groupId and suppress if member was kicked
      if (item.text?.startsWith('[GROUP_INVITE]:')) {
        try {
          const inv = JSON.parse(item.text.replace('[GROUP_INVITE]:', ''));
          if (inv?.groupId) {
            const targetUid = item.receiverId || inv.inviteeId || inv.invitedUserId;
            if (targetUid && kickedPairs.has(`${inv.groupId}_${targetUid}`)) {
              // User was kicked from this group — never return this invite card
              continue;
            }
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
  const ts = readAt ? (readAt instanceof Date ? readAt : new Date(readAt)) : getISTDate();
  if (chatId.startsWith('group_')) {
    await GroupMessage.updateMany(
      { chatId, senderId: { $ne: readerId } },
      { $set: { readAt: ts, status: 'read' } }
    );
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



  // ── REST: Live location state management (ActiveLiveLocation) ──────────────
  app.get('/live-locations/:chatId', async (req, res) => {
    try {
      const chatId = String(req.params.chatId || '').trim();
      if (!chatId) return res.status(400).json({ error: 'chatId is required' });
      const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000 + IST_OFFSET_MS);

      // Clean up expired ones in the background
      ActiveLiveLocation.deleteMany({ chatId, updatedAt: { $lt: threeHoursAgo } }).catch(() => {});

      const docs = await ActiveLiveLocation.find({
        chatId,
        isActive: true,
        updatedAt: { $gte: threeHoursAgo },
      }).lean();

      res.json({ ok: true, locations: docs });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/live-locations/update', async (req, res) => {
    try {
      const { chatId, userId, userName, latitude, longitude, avatarId, photo, isGroup, groupId, groupName, receiverId } = req.body || {};
      if (!chatId || !userId || latitude == null || longitude == null) {
        return res.status(400).json({ error: 'chatId, userId, latitude, longitude are required' });
      }
      const doc = await ActiveLiveLocation.findOneAndUpdate(
        { chatId, userId },
        {
          $set: {
            chatId,
            userId,
            userName: userName || '',
            avatarId: avatarId || null,
            photo: photo || null,
            latitude: Number(latitude),
            longitude: Number(longitude),
            isGroup: Boolean(isGroup || chatId.startsWith('group_')),
            groupId: groupId || (chatId.startsWith('group_') ? chatId.replace(/^group_/, '') : null),
            groupName: groupName || '',
            receiverId: receiverId || null,
            isActive: true,
            updatedAt: getISTDate(),
            istTime: getISTString(),
          },
          $setOnInsert: {
            createdAt: getISTDate(),
          },
        },
        { upsert: true, new: true }
      ).lean();
      res.json({ ok: true, location: doc });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/live-locations/stop', async (req, res) => {
    try {
      const { chatId, userId } = req.body || {};
      if (!chatId || !userId) {
        return res.status(400).json({ error: 'chatId and userId are required' });
      }
      await ActiveLiveLocation.deleteMany({ chatId, userId });
      res.json({ ok: true, stopped: true });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  // ── REST: load messages ──────────────────────────────────────────────────────
  app.get('/messages', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || '').trim();
      if (!chatId) return res.status(400).json({ error: 'chatId is required' });
      const limit = req.query.limit ? parseInt(req.query.limit, 10) : 100;
      const userId = req.query.userId ? String(req.query.userId).trim() : null;
      res.json(await loadMessages(chatId, limit, userId));
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
      } else if (category === 'massage_notification') {
        io.to(chatId).emit('massage_notification:new', record);
      }

      // Also push to receiver's background socket on ANY page (HomeScreen, Friends, etc.)
      const sentSocketIds = new Set();
      if (receiverId && receiverId !== 'all') {
        for (const [sid, meta] of socketMeta.entries()) {
          if (meta.userId === receiverId && meta.chatId !== chatId && !sentSocketIds.has(sid)) {
            const destSock = io.sockets.sockets.get(sid);
            if (destSock) {
              destSock.emit('message:new', record);
              sentSocketIds.add(sid);
              console.log(`[POST /messages] Forwarded message:new to receiver socket sid=${sid} (mode=${meta.mode})`);
            }
          }
        }
      }

      // If a member was kicked from a group, ensure the kicked member's background socket receives it even if not currently in the group room
      const kickedUserId = record.data?.targetUserId || record.data?.kickedUserId;
      if (kickedUserId) {
        for (const [sid, meta] of socketMeta.entries()) {
          if (meta.userId === kickedUserId && meta.chatId !== chatId && !sentSocketIds.has(sid)) {
            const destSock = io.sockets.sockets.get(sid);
            if (destSock) {
              destSock.emit('message:new', record);
              sentSocketIds.add(sid);
              console.log(`[POST /messages] Forwarded member_kicked to kicked user socket sid=${sid} (mode=${meta.mode})`);
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

      // Group invites are sent in 1-on-1 chats and stored in massage_notification
      // Check kicked status in group_notification
      const [massageNotifs, kickedNotifs] = await Promise.all([
        MassageNotification.find({
          receiverId: userId,
          $or: [
            { type: { $in: ['invite', 'group_invite'] } },
            { text: { $regex: '^\\[GROUP_INVITE\\]:' } }
          ]
        }).sort({ createdAt: -1 }).limit(50).lean(),
        GroupNotification.find({
          type: 'member_kicked',
          $or: [
            { 'data.targetUserId': userId },
            { 'data.kickedUserId': userId }
          ]
        }).sort({ createdAt: -1 }).limit(50).lean(),
      ]);

      const kickedGroupIds = new Set(kickedNotifs.map(n => n.groupId || n.data?.groupId).filter(Boolean));
      const invites = [];
      const seenGroupIds = new Set();

      for (const m of massageNotifs) {
        try {
          const inv = JSON.parse(m.text.replace('[GROUP_INVITE]:', ''));
          if (inv?.groupId && !kickedGroupIds.has(inv.groupId) && !seenGroupIds.has(inv.groupId)) {
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

      res.json(invites);
    } catch (err) {
      console.error('GET /group-invites error:', err);
      res.status(500).json({ error: 'Unable to load group invites' });
    }
  });

  // ── REST: load massage notifications strictly (1-on-1 notifications) ───────
  app.get('/massage-notifications', async (req, res) => {
    try {
      const chatId = String(req.query.chatId || '').trim();
      const userId = String(req.query.userId || '').trim();
      const type = req.query.type ? String(req.query.type).trim() : null;
      const limit = req.query.limit ? parseInt(req.query.limit, 10) : 100;
      const parsedLimit = Math.min(Math.max(limit, 1), 500);

      const filter = {};
      if (chatId) {
        filter.chatId = chatId;
      } else {
        filter.chatId = { $not: { $regex: '^group_' } };
      }
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

  // ── REST: get groups for a user ───────────────────────────────────────────
  app.get('/groups', async (req, res) => {
    try {
      const userId = String(req.query.userId || '').trim();
      if (!userId) {
        return res.status(400).json({ error: 'userId is required' });
      }
      const groups = await Group.find({
        'members.id': userId,
        isDeleted: { $ne: true },
      })
        .sort({ lastMessageTime: -1, createdAt: -1 })
        .lean();

      res.json(groups.map(serializeGroup));
    } catch (err) {
      console.error('GET /groups error:', err);
      res.status(500).json({ error: 'Failed to fetch groups' });
    }
  });

  // ── REST: get a single group by id ────────────────────────────────────────
  app.get('/groups/:groupId', async (req, res) => {
    try {
      const groupId = String(req.params.groupId || '').trim();
      if (!groupId) return res.status(400).json({ error: 'groupId is required' });

      const group = await Group.findOne({ id: groupId, isDeleted: { $ne: true } }).lean();
      if (!group) return res.status(404).json({ error: 'Group not found' });

      res.json(serializeGroup(group));
    } catch (err) {
      console.error('GET /groups/:groupId error:', err);
      res.status(500).json({ error: 'Failed to fetch group' });
    }
  });

  // ── REST: create or upsert a group ────────────────────────────────────────
  app.post('/groups', async (req, res) => {
    try {
      const body = req.body || {};
      const groupId = String(body.id || '').trim();
      const name = String(body.name || '').trim();
      const createdBy = String(body.createdBy || '').trim();

      if (!groupId || !name || !createdBy) {
        return res.status(400).json({ error: 'id, name, and createdBy are required' });
      }

      const updateData = {
        id: groupId,
        name,
        description: String(body.description || '').trim(),
        createdBy,
        createdByName: String(body.createdByName || '').trim(),
        createdAt: body.createdAt || getISTString(),
        createdAtDate: getISTDate(),
        updatedAtDate: getISTDate(),
        istTime: getISTString(),
        icon: (body.icon || 'friends').toLowerCase(),
        isDeleted: false,
      };

      if (Array.isArray(body.members) && body.members.length > 0) {
        updateData.members = body.members;
      }
      if (body.lastMessage) updateData.lastMessage = body.lastMessage;
      if (body.lastMessageTime) updateData.lastMessageTime = body.lastMessageTime;

      const group = await Group.findOneAndUpdate(
        { id: groupId },
        { $set: updateData },
        { upsert: true, new: true, setDefaultsOnInsert: true }
      ).lean();

      console.log(`[groups] Upserted group ${groupId} ("${name}") with ${group.members?.length || 0} members`);
      res.json({ ok: true, group: serializeGroup(group) });
    } catch (err) {
      console.error('POST /groups error:', err);
      res.status(500).json({ error: 'Failed to save group' });
    }
  });

  // ── REST: add or update a member in a group ───────────────────────────────
  app.post('/groups/:groupId/member', async (req, res) => {
    try {
      const groupId = String(req.params.groupId || '').trim();
      const { member } = req.body || {};
      if (!groupId || !member?.id) {
        return res.status(400).json({ error: 'groupId and member with id are required' });
      }

      const existing = await Group.findOne({ id: groupId }).lean();
      if (!existing) {
        return res.status(404).json({ error: 'Group not found' });
      }

      const memberObj = {
        id: member.id,
        name: member.name || '',
        avatar_id: member.avatar_id ?? '1',
        photo: member.photo || null,
        role: member.role || 'member',
        joinedAt: member.joinedAt || getISTString(),
      };

      const hasMember = existing.members?.some(m => m.id === member.id);
      let updated;
      if (hasMember) {
        updated = await Group.findOneAndUpdate(
          { id: groupId, 'members.id': member.id },
          { $set: { 'members.$': memberObj } },
          { new: true }
        ).lean();
      } else {
        updated = await Group.findOneAndUpdate(
          { id: groupId },
          { $push: { members: memberObj } },
          { new: true }
        ).lean();
      }

      res.json({ ok: true, group: serializeGroup(updated) });
    } catch (err) {
      console.error('POST /groups/:groupId/member error:', err);
      res.status(500).json({ error: 'Failed to add member to group' });
    }
  });

  // ── REST: remove member from group (leave or kick) ─────────────────────────
  app.post('/groups/:groupId/remove-member', async (req, res) => {
    try {
      const groupId = String(req.params.groupId || '').trim();
      const { userId } = req.body || {};
      if (!groupId || !userId) {
        return res.status(400).json({ error: 'groupId and userId are required' });
      }

      const updated = await Group.findOneAndUpdate(
        { id: groupId },
        { $pull: { members: { id: userId } } },
        { new: true }
      ).lean();

      console.log(`[groups] Removed member ${userId} from group ${groupId}`);
      res.json({ ok: true, group: updated ? serializeGroup(updated) : null });
    } catch (err) {
      console.error('POST /groups/:groupId/remove-member error:', err);
      res.status(500).json({ error: 'Failed to remove member' });
    }
  });

  // ── REST: delete group (soft-delete) ───────────────────────────────────────
  app.delete('/groups/:groupId', async (req, res) => {
    try {
      const groupId = String(req.params.groupId || '').trim();
      if (!groupId) return res.status(400).json({ error: 'groupId is required' });

      await Group.updateOne({ id: groupId }, { $set: { isDeleted: true } });
      io.to(`group_${groupId}`).emit('group:deleted', { groupId });

      console.log(`[groups] Soft-deleted group ${groupId}`);
      res.json({ ok: true, groupId });
    } catch (err) {
      console.error('DELETE /groups/:groupId error:', err);
      res.status(500).json({ error: 'Failed to delete group' });
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

      // 1. Update group document name in groups collection
      await Group.updateOne({ id: groupId }, { $set: { name: cleanName } });

      // 2. Update groupName in all messages for this group in group_massages
      await GroupMessage.updateMany(
        { $or: [{ groupId }, { chatId }] },
        { $set: { groupName: cleanName } }
      );

      // 3. Save a record in group_notification
      const notifDoc = new GroupNotification({
        chatId,
        groupId,
        groupName: cleanName,
        type: 'name_updated',
        senderId: userId || null,
        senderName: userName || 'Admin',
        receiverId: 'all',
        text: `Group name updated to "${cleanName}"`,
        data: {
          groupId,
          groupName: cleanName,
          updatedBy: userId || 'unknown',
          updatedByName: userName || 'Admin',
        },
        deliveredAt: getISTDate(),
        createdAt: getISTDate(),
        istTime: getISTString(),
      });
      await notifDoc.save();

      // 4. Broadcast to socket room
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
        const [delMsgs, delNotifs] = await Promise.all([
          GroupMessage.deleteMany({ chatId }),
          GroupNotification.deleteMany({ chatId }),
        ]);
        const total = (delMsgs.deletedCount || 0) + (delNotifs.deletedCount || 0);
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

      // Also forward to peer socket or reader socket on another screen
      for (const [sid, meta] of socketMeta.entries()) {
        if (meta.chatId !== chatId) {
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
      Group.find({ 'members.id': queryUserId, isDeleted: { $ne: true } })
        .select('id')
        .lean()
        .then((userGroups) => {
          userGroups.forEach((g) => socket.join(`group_${g.id}`));
        })
        .catch(() => {});
    }

    // ── user:register ──────────────────────────────────────────────────────────
    // Called by background socket (HomeScreen) to register for global notifications
    socket.on('user:register', async ({ userId } = {}) => {
      if (!userId) return;
      const meta = socketMeta.get(socket.id) || {};
      socketMeta.set(socket.id, { ...meta, userId, mode: 'background' });
      setUserOnline(userId, socket.id, null, null);
      console.log(`[user:register] userId=${userId} background socket registered`);

      try {
        const userGroups = await Group.find({ 'members.id': userId, isDeleted: { $ne: true } })
          .select('id')
          .lean();
        userGroups.forEach((g) => socket.join(`group_${g.id}`));
      } catch (e) {}
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
        // Also forward to background/other sockets outside the room
        for (const [sid, meta] of socketMeta.entries()) {
          if (meta.chatId !== roomId) {
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
        } else if (category === 'massage_notification') {
          io.to(roomId).emit('massage_notification:new', record);
        }

        // ── In-app toast: find receiver's socket and emit directly on ANY page ──
        const sentSocketIds = new Set();
        if (receiverId && receiverId !== 'all') {
          for (const [sid, meta] of socketMeta.entries()) {
            if (meta.userId === receiverId && meta.chatId !== roomId && !sentSocketIds.has(sid)) {
              const bgSock = io.sockets.sockets.get(sid);
              if (bgSock) {
                bgSock.emit('message:new', record);
                sentSocketIds.add(sid);
                console.log(`[toast] Sent message:new to receiver socket of userId=${receiverId} (sid=${sid}, mode=${meta.mode})`);
              }
            }
          }
        }

        // If member was kicked from a group, ensure the kicked member receives it via their background socket
        const kickedUserId = record.data?.targetUserId || record.data?.kickedUserId;
        if (kickedUserId) {
          for (const [sid, meta] of socketMeta.entries()) {
            if (meta.userId === kickedUserId && meta.chatId !== roomId && !sentSocketIds.has(sid)) {
              const bgSock = io.sockets.sockets.get(sid);
              if (bgSock) {
                bgSock.emit('message:new', record);
                sentSocketIds.add(sid);
                console.log(`[toast] Sent message:new (member_kicked) to kicked user socket sid=${sid}`);
              }
            }
          }
        }

        // ── Send FCM push notification (mobile system tray) via firebase-admin ─────
        if ((category === 'message' || category === 'massage_notification') && receiverId) {
          const receiverMeta = userPresence.get(receiverId);
          const receiverInRoom = receiverMeta?.chatId === roomId && receiverMeta?.isActive;
          if (!receiverInRoom) {
            let pushBody = text || '';
            if (pushBody.startsWith('[LIVE_LOCATION]:') || pushBody.startsWith('[GROUP_LOCATION]:')) {
              pushBody = '📍 Shared live location';
            } else if (
              pushBody.startsWith('[LIVE_LOCATION_STOPPED]:') ||
              pushBody.startsWith('[GROUP_LOCATION_STOPPED]:') ||
              pushBody.startsWith('[LOCATION_STOPPED]:')
            ) {
              pushBody = 'Stopped sharing live location';
            }

            if (pushBody && !pushBody.startsWith('[')) {
              const fcmToken = await getPushToken(receiverId);
              await sendFCMNotification(
                fcmToken,
                senderName || 'New message',
                pushBody.length > 100 ? pushBody.slice(0, 97) + '…' : pushBody,
                { otherUserId: senderId, senderName: senderName || 'Someone', chatId: roomId }
              );
            }
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
          groupName: cleanName,
          type: 'name_updated',
          senderId: userId || null,
          senderName: userName || 'Admin',
          receiverId: 'all',
          text: `Group name updated to "${cleanName}"`,
          data: {
            groupId,
            groupName: cleanName,
            updatedBy: userId || 'unknown',
            updatedByName: userName || 'Admin',
          },
          deliveredAt: getISTDate(),
          createdAt: getISTDate(),
          istTime: getISTString(),
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
