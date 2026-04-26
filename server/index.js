const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const protobuf = require('protobufjs');
const path = require('path');

// --- 1. CONFIG & STATE ---
const WS_PORT = 8080;
const BROADCAST_MS = 100;
let TickMessage = null;
let marketSnapshot = {}; 
let stats = { count: 0, lastLogCount: 0 };
const startTime = Date.now(); 

// --- 2. LOAD PROTOBUF ---
protobuf.load(path.join(__dirname, "tick.proto"), (err, root) => {
    if (err) {
        console.error("❌ PROTOBUF LOAD ERROR:", err);
        process.exit(1);
    }
    TickMessage = root.lookupType("Tick");
    console.log("✅ Protobuf Schema Loaded: 'Tick' found.");
});

// --- 3. WEBSOCKET SERVER ---
const wss = new WebSocket.Server({ port: WS_PORT });
let clients = new Set();
wss.on('connection', (ws) => {
    clients.add(ws);
    console.log(`🌐 UI Connected. Total clients: ${clients.size}`);
    ws.on('close', () => clients.delete(ws));
});

// --- 4. THE DOWNSAMPLER (Fixed negative timeout math) ---
setInterval(() => {
    if (clients.size > 0 && Object.keys(marketSnapshot).length > 0) {
        const payload = JSON.stringify({ 
            type: 'UPDATE', 
            data: marketSnapshot, 
            total: stats.count 
        });
        for (let client of clients) {
            if (client.readyState === WebSocket.OPEN) client.send(payload);
        }
    }
}, BROADCAST_MS);

// --- 5. KAFKA ENGINE ---
const kafka = new Kafka({ 
    clientId: 'aggregator-v2', 
    brokers: ['127.0.0.1:9092'],
    retry: { initialRetryTime: 100, retries: 8 }
});

const consumer = kafka.consumer({ groupId: 'bullpen-aggregator-v2' });

const run = async () => {
    console.log("⚡ Connecting to Kafka...");
    await consumer.connect();
    await consumer.subscribe({ topic: 'crypto-ticks', fromBeginning: false });
    
    console.log("🚀 Aggregator Online. Listening for messages...");

    await consumer.run({
        eachBatch: async ({ batch }) => {
            // DEBUG: See if we are getting anything at all
            if (batch.messages.length > 0) {
                // console.log(`📦 Received batch of ${batch.messages.length} messages`);
            }

            for (let message of batch.messages) {
                if (!TickMessage) continue;

                try {
                    const tick = TickMessage.decode(message.value);
                    marketSnapshot[tick.symbol] = { 
                        p: tick.price.toFixed(2), 
                        t: tick.timestamp.toString() 
                    };
                    stats.count++;
                } catch (e) {
                    console.error("❌ Decode Error:", e.message);
                }
            }

            // Log every 10,000 ticks instead of 100,000 for faster feedback
            if (stats.count >= stats.lastLogCount + 10000) {
                console.log(`📥 Ingested: ${stats.count.toLocaleString()} ticks...`);
                stats.lastLogCount = stats.count;
            }
        }
    });
};

// Keep process alive and catch top-level crashes
run().catch(err => {
    console.error("🔥 CRITICAL SERVER CRASH:", err);
    process.exit(1);
});