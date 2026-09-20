const path = require('path');
const fs = require('fs');

// Resolve mongodb from root or server
let MongoClient;
try {
  MongoClient = require('mongodb').MongoClient;
} catch (e) {
  try {
    MongoClient = require(path.resolve(__dirname, '..', 'node_modules', 'mongodb')).MongoClient;
  } catch (e2) {
    MongoClient = require('c:/Puja/node_modules/mongodb').MongoClient;
  }
}

// Read .env
const envPath = path.resolve(__dirname, '..', '.env');
const envContent = fs.readFileSync(envPath, 'utf8');
let MONGODB_URI = '';
let MONGODB_DB = 'chat_puja';
for (const line of envContent.split('\n')) {
  if (line.startsWith('MONGODB_URI=')) MONGODB_URI = line.replace('MONGODB_URI=', '').trim();
  if (line.startsWith('MONGODB_DB=')) MONGODB_DB = line.replace('MONGODB_DB=', '').trim();
}

function isNotificationPacket(text) {
  if (!text) return false;
  return text.startsWith('[GROUP_');
}

function parseGroupNotification(doc) {
  let type = 'general';
  let data = null;
  const text = doc.text || '';
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
  }

  const groupId = (doc.chatId && doc.chatId.startsWith('group_'))
    ? doc.chatId.replace(/^group_/, '')
    : (data?.groupId || null);

  return {
    chatId: doc.chatId,
    groupId,
    type,
    senderId: doc.senderId,
    senderName: doc.senderName,
    receiverId: doc.receiverId || 'all',
    text: doc.text,
    data,
    clientMessageId: doc.clientMessageId || null,
    createdAt: doc.createdAt || new Date(),
  };
}

async function migrate() {
  console.log(' Connecting to MongoDB Atlas...');
  console.log(` Database: ${MONGODB_DB}`);

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const db = client.db(MONGODB_DB);

  console.log(' Connected successfully.');

  // Collections
  const messagesCol = db.collection('messages');
  const groupMessagesCol = db.collection('group_massages');
  const groupNotificationsCol = db.collection('group_notification');
  const notifyTokenCol = db.collection('notifytoken');

  // Create collections if they don't exist
  const existingCols = (await db.listCollections().toArray()).map((c) => c.name);
  console.log(' Existing collections in database:', existingCols);

  if (!existingCols.includes('group_massages')) {
    await db.createCollection('group_massages');
    console.log(' Created collection: group_massages');
  }
  if (!existingCols.includes('group_notification')) {
    await db.createCollection('group_notification');
    console.log(' Created collection: group_notification');
  }

  // Setup Indexes
  console.log('\n Creating / Ensuring indexes...');
  // 1. messages
  await messagesCol.createIndex({ chatId: 1, createdAt: 1 });
  await messagesCol.createIndex({ chatId: 1, createdAt: -1 });
  await messagesCol.createIndex({ receiverId: 1 });

  // 2. group_massages
  await groupMessagesCol.createIndex({ chatId: 1, createdAt: 1 });
  await groupMessagesCol.createIndex({ groupId: 1, createdAt: -1 });
  await groupMessagesCol.createIndex({ senderId: 1 });

  // 3. group_notification
  await groupNotificationsCol.createIndex({ chatId: 1, createdAt: -1 });
  await groupNotificationsCol.createIndex({ groupId: 1, type: 1 });
  await groupNotificationsCol.createIndex({ type: 1, createdAt: -1 });
  // TTL index: auto-expire ephemeral live location updates after 24 hours (86400s)
  await groupNotificationsCol.createIndex(
    { createdAt: 1 },
    { expireAfterSeconds: 86400 * 3, partialFilterExpression: { type: { $in: ['location', 'location_stopped'] } } }
  );

  // 4. notifytoken
  await notifyTokenCol.createIndex({ userId: 1 }, { unique: true });

  console.log(' All collection indexes created successfully.');

  // Find all group documents currently in messages
  const groupDocs = await messagesCol.find({
    $or: [
      { chatId: { $regex: '^group_' } },
      { text: { $regex: '^\\[GROUP_' } },
    ],
  }).toArray();

  console.log(`\n Found ${groupDocs.length} group/system documents currently in 'messages'.`);

  if (groupDocs.length > 0) {
    const toGroupMessages = [];
    const toGroupNotifications = [];
    const idsToRemove = [];

    for (const doc of groupDocs) {
      idsToRemove.push(doc._id);

      if (isNotificationPacket(doc.text)) {
        toGroupNotifications.push(parseGroupNotification(doc));
      } else {
        const groupId = doc.chatId && doc.chatId.startsWith('group_')
          ? doc.chatId.replace(/^group_/, '')
          : null;

        toGroupMessages.push({
          chatId: doc.chatId,
          groupId,
          senderId: doc.senderId,
          senderName: doc.senderName,
          text: doc.text,
          clientMessageId: doc.clientMessageId || null,
          deliveredAt: doc.deliveredAt || doc.createdAt,
          readAt: doc.readAt || null,
          createdAt: doc.createdAt,
        });
      }
    }

    console.log(` -> Migrating ${toGroupMessages.length} documents into 'group_massages'...`);
    if (toGroupMessages.length > 0) {
      await groupMessagesCol.insertMany(toGroupMessages);
      console.log(`    Successfully inserted ${toGroupMessages.length} into 'group_massages'.`);
    }

    console.log(` -> Migrating ${toGroupNotifications.length} documents into 'group_notification'...`);
    if (toGroupNotifications.length > 0) {
      await groupNotificationsCol.insertMany(toGroupNotifications);
      console.log(`    Successfully inserted ${toGroupNotifications.length} into 'group_notification'.`);
    }

    console.log(` -> Removing ${idsToRemove.length} group documents from 'messages'...`);
    const delResult = await messagesCol.deleteMany({ _id: { $in: idsToRemove } });
    console.log(`    Removed ${delResult.deletedCount} documents from 'messages'.`);
  } else {
    console.log(' No group documents needed migration from messages.');
  }

  // Verification counts
  console.log('\n================ FINAL COLLECTION STATUS ================');
  const messagesCount = await messagesCol.countDocuments();
  const groupMsgCount = await groupMessagesCol.countDocuments();
  const groupNotifCount = await groupNotificationsCol.countDocuments();
  const notifyTokenCount = await notifyTokenCol.countDocuments();

  console.log(`1. messages (personal 1-on-1 messages):   ${messagesCount} documents`);
  console.log(`2. group_massages (group chat messages):    ${groupMsgCount} documents`);
  console.log(`3. group_notification (group notifications): ${groupNotifCount} documents`);
  console.log(`4. notifytoken (device push tokens):        ${notifyTokenCount} documents`);
  console.log('=========================================================\n');

  await client.close();
  console.log(' Migration complete.');
}

migrate().catch((err) => {
  console.error(' Migration failed:', err);
  process.exit(1);
});
