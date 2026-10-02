/**
 * Unified Database Tool for Puja App Chat & Notifications
 * 
 * Merged utility providing:
 *  1. Inspection & Health Status (inspect/status)
 *  2. Schema Migration & Separation Cleanup (migrate/clean)
 *  3. End-to-End Collection Isolation Testing (test)
 * 
 * Usage:
 *   node db_tool.js          -> Check collection status & health
 *   node db_tool.js inspect  -> Detailed inspection with sample docs
 *   node db_tool.js migrate  -> Run migration, indexing, and cleanup
 *   node db_tool.js test     -> Run end-to-end 4-way isolation tests
 *   node db_tool.js all      -> Migrate, inspect, and test
 */

const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');

// ── Environment Configuration ──────────────────────────────────────────────────
let MONGODB_URI = process.env.MONGODB_URI || '';
let MONGODB_DB = process.env.MONGODB_DB || 'chat_puja';

const envPath = path.resolve(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.startsWith('MONGODB_URI=')) {
      MONGODB_URI = trimmed.replace('MONGODB_URI=', '').trim();
    }
    if (trimmed.startsWith('MONGODB_DB=')) {
      MONGODB_DB = trimmed.replace('MONGODB_DB=', '').trim();
    }
  }
}

if (!MONGODB_URI) {
  console.error('Error: MONGODB_URI not found in environment or .env');
  process.exit(1);
}

// ── Packet Classification Helpers ─────────────────────────────────────────────
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

function parseMassageNotificationDoc(doc) {
  let type = 'general';
  let data = null;
  let groupId = null;
  let groupName = '';
  const text = doc.text || '';

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

  return {
    chatId: doc.chatId,
    type,
    senderId: doc.senderId || null,
    senderName: doc.senderName || 'Unknown',
    receiverId: doc.receiverId || null,
    groupId,
    groupName,
    text: doc.text,
    data,
    clientMessageId: doc.clientMessageId || null,
    deliveredAt: doc.deliveredAt || doc.createdAt || new Date(),
    readAt: doc.readAt || null,
    createdAt: doc.createdAt || new Date(),
  };
}

function parseGroupNotificationDoc(doc) {
  let type = doc.type || 'general';
  let data = doc.data || null;
  const text = doc.text || '';
  let groupId = doc.groupId || (doc.chatId && doc.chatId.startsWith('group_') ? doc.chatId.replace(/^group_/, '') : null);
  let groupName = doc.groupName || '';

  if (!doc.type || doc.type === 'general') {
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
    }
  }

  return {
    chatId: doc.chatId,
    groupId,
    groupName,
    type,
    senderId: doc.senderId || null,
    senderName: doc.senderName || 'System',
    receiverId: doc.receiverId || 'all',
    text: doc.text,
    data,
    clientMessageId: doc.clientMessageId || null,
    createdAt: doc.createdAt || new Date(),
  };
}

// ── Operation: Status / Inspect ───────────────────────────────────────────────
async function checkStatus(db, detailed = false) {
  console.log('\n================ DATABASE HEALTH & STATUS ================');
  console.log(`Database: ${MONGODB_DB}`);

  const messagesCol = db.collection('messages');
  const groupMessagesCol = db.collection('group_massages');
  const groupNotificationsCol = db.collection('group_notification');
  const massageNotificationsCol = db.collection('massage_notification');
  const notifyTokenCol = db.collection('notifytoken');

  const countMessages = await messagesCol.countDocuments();
  const countGroupMessages = await groupMessagesCol.countDocuments();
  const countGroupNotifications = await groupNotificationsCol.countDocuments();
  const countMassageNotifications = await massageNotificationsCol.countDocuments();
  const countTokens = await notifyTokenCol.countDocuments();

  // Cross-pollution checks
  const nonPersonalInMessages = await messagesCol.countDocuments({
    $or: [
      { chatId: { $regex: '^group_' } },
      { text: { $regex: '^\\[.*?\\]:' } }
    ]
  });

  const nonGroupInGroupMassages = await groupMessagesCol.countDocuments({
    $or: [
      { chatId: { $not: { $regex: '^group_' } } },
      { text: { $regex: '^\\[.*?\\]:' } }
    ]
  });

  const nonGroupInGroupNotif = await groupNotificationsCol.countDocuments({
    chatId: { $not: { $regex: '^group_' } }
  });

  const groupInMassageNotif = await massageNotificationsCol.countDocuments({
    chatId: { $regex: '^group_' }
  });

  console.log(`1. messages (Personal 1-on-1 text messages):`);
  console.log(`   Count: ${countMessages} | Invalid/Polluted: ${nonPersonalInMessages}`);

  console.log(`2. group_massages (Group chat text messages):`);
  console.log(`   Count: ${countGroupMessages} | Invalid/Polluted: ${nonGroupInGroupMassages}`);

  console.log(`3. group_notification (Group events & notifications):`);
  console.log(`   Count: ${countGroupNotifications} | Invalid/Polluted: ${nonGroupInGroupNotif}`);

  console.log(`4. massage_notification (1-on-1 live location, group invites):`);
  console.log(`   Count: ${countMassageNotifications} | Invalid/Polluted: ${groupInMassageNotif}`);

  console.log(`5. notifytoken (Device push tokens):`);
  console.log(`   Count: ${countTokens}`);

  const isHealthy = (nonPersonalInMessages === 0 && nonGroupInGroupMassages === 0 && nonGroupInGroupNotif === 0 && groupInMassageNotif === 0);
  console.log(`\nOverall Health: ${isHealthy ? 'HEALTHY (100% strict separation)' : 'WARNING (Migration/Cleanup needed)'}`);
  console.log('==========================================================\n');

  if (detailed) {
    console.log('--- Sample Recent Documents ---');
    const msg = await messagesCol.find().sort({ createdAt: -1 }).limit(1).toArray();
    if (msg.length) console.log('Sample message:', msg[0].chatId, msg[0].text);
    const gmsg = await groupMessagesCol.find().sort({ createdAt: -1 }).limit(1).toArray();
    if (gmsg.length) console.log('Sample group message:', gmsg[0].chatId, gmsg[0].text);
    const gnotif = await groupNotificationsCol.find().sort({ createdAt: -1 }).limit(1).toArray();
    if (gnotif.length) console.log('Sample group notification:', gnotif[0].chatId, gnotif[0].type, gnotif[0].text.slice(0, 60));
    const mnotif = await massageNotificationsCol.find().sort({ createdAt: -1 }).limit(1).toArray();
    if (mnotif.length) console.log('Sample 1-on-1 notification:', mnotif[0].chatId, mnotif[0].type, mnotif[0].text.slice(0, 60));
    console.log('');
  }

  return isHealthy;
}

// ── Operation: Migration & Cleanup ────────────────────────────────────────────
async function runMigration(db) {
  console.log('\n--- Starting Migration & Separation Cleanup ---');
  const messagesCol = db.collection('messages');
  const groupMessagesCol = db.collection('group_massages');
  const groupNotificationsCol = db.collection('group_notification');
  const massageNotificationsCol = db.collection('massage_notification');

  // 1. Move any notifications in 'messages' to 'massage_notification'
  const notifsInMessages = await messagesCol.find({
    $or: [
      { text: { $regex: '^\\[LIVE_LOCATION\\]:' } },
      { text: { $regex: '^\\[LIVE_LOCATION_STOPPED\\]:' } },
      { text: { $regex: '^\\[LOCATION_STOPPED\\]:' } },
      { text: { $regex: '^\\[GROUP_INVITE\\]:' } },
      { text: { $regex: '^\\[GROUP_INVITE_ACCEPTED\\]:' } },
      { text: { $regex: '^\\[GROUP_INVITE_REJECTED\\]:' } },
    ]
  }).toArray();

  if (notifsInMessages.length > 0) {
    const toInsert = notifsInMessages.map(parseMassageNotificationDoc);
    const validInserts = [];
    for (const item of toInsert) {
      if (item.clientMessageId) {
        const exists = await massageNotificationsCol.findOne({ clientMessageId: item.clientMessageId });
        if (!exists) validInserts.push(item);
      } else {
        validInserts.push(item);
      }
    }
    if (validInserts.length > 0) {
      await massageNotificationsCol.insertMany(validInserts);
    }
    await messagesCol.deleteMany({ _id: { $in: notifsInMessages.map(d => d._id) } });
    console.log(`  Migrated & purged ${notifsInMessages.length} notifications from 'messages' -> 'massage_notification'.`);
  } else {
    console.log('  0 notifications found in messages.');
  }

  // 2. Move any group messages in 'messages' to 'group_massages'
  const groupInMessages = await messagesCol.find({ chatId: { $regex: '^group_' } }).toArray();
  if (groupInMessages.length > 0) {
    await groupMessagesCol.insertMany(groupInMessages);
    await messagesCol.deleteMany({ _id: { $in: groupInMessages.map(d => d._id) } });
    console.log(`  Migrated & purged ${groupInMessages.length} group messages from 'messages' -> 'group_massages'.`);
  } else {
    console.log('  0 group messages found in messages.');
  }

  // 3. Move any group notifications from 'massage_notification' to 'group_notification'
  const groupInMN = await massageNotificationsCol.find({
    $or: [
      { chatId: { $regex: '^group_' } },
      { text: { $regex: '^\\[GROUP_LOCATION\\]:' } },
      { text: { $regex: '^\\[GROUP_LOCATION_STOPPED\\]:' } },
      { text: { $regex: '^\\[GROUP_MEMBER_SYNC\\]:' } },
      { text: { $regex: '^\\[GROUP_MEMBER_LEFT\\]:' } },
      { text: { $regex: '^\\[GROUP_MEMBER_KICKED\\]:' } },
      { text: { $regex: '^\\[GROUP_NAME_UPDATED\\]:' } },
      { text: { $regex: '^\\[GROUP_SYSTEM\\]:' } },
    ]
  }).toArray();

  if (groupInMN.length > 0) {
    const toInsertGN = [];
    for (const doc of groupInMN) {
      const parsed = parseGroupNotificationDoc(doc);
      let exists = false;
      if (parsed.clientMessageId) {
        exists = await groupNotificationsCol.findOne({ clientMessageId: parsed.clientMessageId });
      }
      if (!exists) {
        exists = await groupNotificationsCol.findOne({
          chatId: parsed.chatId,
          text: parsed.text,
          createdAt: {
            $gte: new Date(new Date(parsed.createdAt).getTime() - 5000),
            $lte: new Date(new Date(parsed.createdAt).getTime() + 5000),
          }
        });
      }
      if (!exists) toInsertGN.push(parsed);
    }
    if (toInsertGN.length > 0) {
      await groupNotificationsCol.insertMany(toInsertGN);
    }
    await massageNotificationsCol.deleteMany({ _id: { $in: groupInMN.map(d => d._id) } });
    console.log(`  Migrated & purged ${groupInMN.length} group records from 'massage_notification' -> 'group_notification'.`);
  } else {
    console.log('  0 group records found in massage_notification.');
  }

  // 4. Ensure optimal indexes on all 4 collections
  console.log('  Ensuring collection indexes...');
  await messagesCol.createIndex({ chatId: 1, createdAt: 1 });
  await messagesCol.createIndex({ chatId: 1, createdAt: -1 });
  await messagesCol.createIndex({ receiverId: 1 });

  await groupMessagesCol.createIndex({ chatId: 1, createdAt: 1 });
  await groupMessagesCol.createIndex({ groupId: 1, createdAt: -1 });

  await groupNotificationsCol.createIndex({ chatId: 1, createdAt: -1 });
  await groupNotificationsCol.createIndex({ groupId: 1, type: 1 });
  await groupNotificationsCol.createIndex({ type: 1, createdAt: -1 });
  await groupNotificationsCol.createIndex({ type: 1, 'data.targetUserId': 1 });
  await groupNotificationsCol.createIndex({ type: 1, 'data.kickedUserId': 1 });
  // Drop older conflicting TTL index if present on group_notification
  try {
    const existingGroupIndexes = await groupNotificationsCol.indexes();
    if (existingGroupIndexes.some(idx => idx.name === 'createdAt_1')) {
      await groupNotificationsCol.dropIndex('createdAt_1');
    }
  } catch (e) {}

  await groupNotificationsCol.createIndex(
    { createdAt: 1 },
    { expireAfterSeconds: 86400, partialFilterExpression: { type: { $in: ['location', 'location_stopped'] } } }
  );

  await massageNotificationsCol.createIndex({ chatId: 1, createdAt: -1 });
  await massageNotificationsCol.createIndex({ receiverId: 1, type: 1 });
  await massageNotificationsCol.createIndex({ groupId: 1, createdAt: -1 });

  // Drop older conflicting TTL index if present on massage_notification
  try {
    const existingMassageIndexes = await massageNotificationsCol.indexes();
    if (existingMassageIndexes.some(idx => idx.name === 'createdAt_1')) {
      await massageNotificationsCol.dropIndex('createdAt_1');
    }
  } catch (e) {}

  await massageNotificationsCol.createIndex(
    { createdAt: 1 },
    { expireAfterSeconds: 86400, partialFilterExpression: { type: { $in: ['live_location', 'live_location_stopped', 'location_stopped'] } } }
  );

  console.log('Migration & cleanup finished successfully.');
  await checkStatus(db);
}

// ── Operation: Isolation Test ──────────────────────────────────────────────────
async function runTest(db) {
  console.log('\n--- Running 4-Collection End-to-End Isolation Test ---');
  const messagesCol = db.collection('messages');
  const groupMessagesCol = db.collection('group_massages');
  const groupNotificationsCol = db.collection('group_notification');
  const massageNotificationsCol = db.collection('massage_notification');

  const testId = `test_${Date.now()}`;
  const userA = 'user_test_a';
  const userB = 'user_test_b';
  const chat1on1 = [userA, userB].sort().join('_');
  const testGroupId = `grp_test_${Date.now()}`;
  const groupChatId = `group_${testGroupId}`;

  // 1. Personal Message -> messages
  await messagesCol.insertOne({
    chatId: chat1on1,
    senderId: userA,
    senderName: 'User A',
    receiverId: userB,
    text: `Hello User B personal text ${testId}`,
    clientMessageId: `msg_${testId}`,
    deliveredAt: new Date(),
    readAt: null,
    createdAt: new Date(),
  });

  // 2. 1-on-1 Live Location & Group Invite -> massage_notification
  await massageNotificationsCol.insertMany([
    {
      chatId: chat1on1,
      type: 'live_location',
      senderId: userA,
      senderName: 'User A',
      receiverId: userB,
      groupId: null,
      groupName: '',
      text: `[LIVE_LOCATION]:{"latitude":22.5,"longitude":88.3}`,
      data: { latitude: 22.5, longitude: 88.3 },
      clientMessageId: `loc_${testId}`,
      deliveredAt: new Date(),
      createdAt: new Date(),
    },
    {
      chatId: chat1on1,
      type: 'invite',
      senderId: userA,
      senderName: 'User A',
      receiverId: userB,
      groupId: testGroupId,
      groupName: 'Test Group',
      text: `[GROUP_INVITE]:{"groupId":"${testGroupId}","groupName":"Test Group"}`,
      data: { groupId: testGroupId, groupName: 'Test Group' },
      clientMessageId: `inv_${testId}`,
      deliveredAt: new Date(),
      createdAt: new Date(),
    }
  ]);

  // 3. Group Message -> group_massages
  await groupMessagesCol.insertOne({
    chatId: groupChatId,
    groupId: testGroupId,
    groupName: 'Test Group',
    senderId: userA,
    senderName: 'User A',
    text: `Group chat text message ${testId}`,
    clientMessageId: `grpmsg_${testId}`,
    deliveredAt: new Date(),
    createdAt: new Date(),
  });

  // 4. Group Notifications (Member Join & Group Live Location) -> group_notification
  await groupNotificationsCol.insertMany([
    {
      chatId: groupChatId,
      groupId: testGroupId,
      groupName: 'Test Group',
      type: 'member_sync',
      senderId: userB,
      senderName: 'User B',
      receiverId: 'all',
      text: `[GROUP_MEMBER_SYNC]:{"groupId":"${testGroupId}","member":{"id":"${userB}"}}`,
      data: { groupId: testGroupId, member: { id: userB } },
      clientMessageId: `sync_${testId}`,
      createdAt: new Date(),
    },
    {
      chatId: groupChatId,
      groupId: testGroupId,
      groupName: 'Test Group',
      type: 'location',
      senderId: userA,
      senderName: 'User A',
      receiverId: 'all',
      text: `[GROUP_LOCATION]:{"userId":"${userA}","latitude":22.57,"longitude":88.36}`,
      data: { userId: userA, latitude: 22.57, longitude: 88.36 },
      clientMessageId: `grploc_${testId}`,
      createdAt: new Date(),
    }
  ]);

  // Verification
  const fMsg = await messagesCol.findOne({ clientMessageId: `msg_${testId}` });
  const fLoc = await massageNotificationsCol.findOne({ clientMessageId: `loc_${testId}` });
  const fInv = await massageNotificationsCol.findOne({ clientMessageId: `inv_${testId}` });
  const fGrpMsg = await groupMessagesCol.findOne({ clientMessageId: `grpmsg_${testId}` });
  const fSync = await groupNotificationsCol.findOne({ clientMessageId: `sync_${testId}` });
  const fGrpLoc = await groupNotificationsCol.findOne({ clientMessageId: `grploc_${testId}` });

  // Cross pollution checks
  const pMsg = await messagesCol.findOne({ clientMessageId: { $in: [`loc_${testId}`, `inv_${testId}`, `grpmsg_${testId}`, `sync_${testId}`, `grploc_${testId}`] } });
  const pGrpMsg = await groupMessagesCol.findOne({ clientMessageId: { $in: [`msg_${testId}`, `loc_${testId}`, `inv_${testId}`, `sync_${testId}`, `grploc_${testId}`] } });
  const pGNotif = await groupNotificationsCol.findOne({ clientMessageId: { $in: [`msg_${testId}`, `loc_${testId}`, `inv_${testId}`, `grpmsg_${testId}`] } });
  const pMNotif = await massageNotificationsCol.findOne({ clientMessageId: { $in: [`msg_${testId}`, `grpmsg_${testId}`, `sync_${testId}`, `grploc_${testId}`] } });

  // Cleanup
  await messagesCol.deleteOne({ clientMessageId: `msg_${testId}` });
  await massageNotificationsCol.deleteMany({ clientMessageId: { $in: [`loc_${testId}`, `inv_${testId}`] } });
  await groupMessagesCol.deleteOne({ clientMessageId: `grpmsg_${testId}` });
  await groupNotificationsCol.deleteMany({ clientMessageId: { $in: [`sync_${testId}`, `grploc_${testId}`] } });

  const passed = Boolean(fMsg && fLoc && fInv && fGrpMsg && fSync && fGrpLoc && !pMsg && !pGrpMsg && !pGNotif && !pMNotif);
  console.log(`Test Result: ${passed ? 'PASSED (Zero cross-pollution across all 4 collections)' : 'FAILED'}`);
  return passed;
}

// ── Main CLI Runner ────────────────────────────────────────────────────────────
async function main() {
  const arg = (process.argv[2] || 'status').toLowerCase();
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const db = client.db(MONGODB_DB);

  try {
    if (arg === 'status') {
      await checkStatus(db, false);
    } else if (arg === 'inspect') {
      await checkStatus(db, true);
    } else if (arg === 'migrate' || arg === 'clean') {
      await runMigration(db);
    } else if (arg === 'test') {
      await runTest(db);
    } else if (arg === 'all') {
      await runMigration(db);
      await runTest(db);
      await checkStatus(db, true);
    } else {
      console.log(`Unknown command "${arg}". Available: status, inspect, migrate, test, all`);
    }
  } finally {
    await client.close();
  }
}

main().catch(err => {
  console.error('Fatal db_tool error:', err);
  process.exit(1);
});
