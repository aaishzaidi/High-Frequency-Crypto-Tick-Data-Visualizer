const { Kafka } = require('kafkajs');
const WebSocket = require('ws');

// --- 1. WebSocket Server Setup ---
// This opens a port specifically for our React frontend to connect to
const wss = new WebSocket.Server({ port: 8080 });
let clients = [];

wss.on('connection', (ws) => {
    console.log('🌐 React frontend client connected!');
    clients.push(ws);
    
    ws.on('close', () => {
        clients = clients.filter(client => client !== ws);
        console.log('🌐 Frontend client disconnected.');
    });
});

// --- 2. Kafka Setup ---
const kafka = new Kafka({
  clientId: 'node-relay-server',
  brokers: ['127.0.0.1:9092'] 
});

const consumer = kafka.consumer({ groupId: 'crypto-group' });

// --- 3. The Batching Engine ---
let tickBatch = [];
const BATCH_INTERVAL_MS = 100; // Send data to frontend every 100ms (10 times per sec)

// This interval loop acts like a heartbeat, flushing the batch to React
setInterval(() => {
    if (tickBatch.length > 0 && clients.length > 0) {
        const payload = JSON.stringify(tickBatch);
        
        // Broadcast the batch to all connected web browsers
        clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(payload);
            }
        });
    }
    // Always clear the batch to prevent server memory leaks!
    tickBatch = []; 
}, BATCH_INTERVAL_MS);


// --- 4. Kafka Consumer Execution ---
let messageCount = 0;

const run = async () => {
  try {
    await consumer.connect();
    console.log('🔌 Connected to Kafka successfully.');

    await consumer.subscribe({ topic: 'crypto-ticks', fromBeginning: false });
    console.log('📡 Subscribed to [crypto-ticks]. Waiting for data stream...');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        messageCount++;
        const tick = JSON.parse(message.value.toString());
        
        // Push the new tick into our temporary batch array
        tickBatch.push(tick);
        
        if (messageCount % 10000 === 0) {
           console.log(`📦 Processed 10,000 ticks. Active WebSockets: ${clients.length}`);
        }
      },
    });
  } catch (error) {
    console.error(`❌ Error connecting to Kafka: ${error}`);
  }
};

run().catch(console.error);