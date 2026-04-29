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
let isConsumerPaused = false; // The Circuit Breaker Flag

// --- 2. LOAD PROTOBUF ---
protobuf.load(path.join(__dirname, "tick.proto"), (err, root) => {
    if (err) {
        console.error("❌ PROTOBUF LOAD ERROR:", err);
        process.exit(1);
    }
    TickMessage = root.lookupType("Tick");
    console.log("✅ Protobuf Schema Loaded: 'Tick' found.");
});

// --- 3. KAFKA ENGINE SETUP ---
const kafka = new Kafka({ clientId: 'bullpen-node', brokers: ['127.0.0.1:9092'] });
const consumer = kafka.consumer({ groupId: 'bullpen-group-' + Date.now() });

// --- 4. WEBSOCKET SERVER (With Circuit Breaker Hook) ---
const wss = new WebSocket.Server({ port: WS_PORT });
let clients = new Set();

wss.on('connection', (ws) => {
    clients.add(ws);
    console.log(`🌐 UI Connected. Total clients: ${clients.size}`);
    
    // Listen for Kill Switch signals from the React UI
    ws.on('message', (data) => {
        const message = JSON.parse(data);
        if (message.action === 'HALT' && !isConsumerPaused) {
            console.log("🛑 CIRCUIT BREAKER PULLED: Pausing Kafka Ingestion...");
            consumer.pause([{ topic: 'crypto-ticks' }]);
            isConsumerPaused = true;
        } else if (message.action === 'RESUME' && isConsumerPaused) {
            console.log("🟢 SYSTEM ONLINE: Resuming Kafka Ingestion...");
            consumer.resume([{ topic: 'crypto-ticks' }]);
            isConsumerPaused = false;
        }
    });

    ws.on('close', () => clients.delete(ws));
});

// --- 5. THE DOWNSAMPLER ---
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

// --- 6. RUN KAFKA CONSUMER ---
const run = async () => {
    console.log("⚡ Connecting to Kafka...");
    await consumer.connect();
    await consumer.subscribe({ topic: 'crypto-ticks', fromBeginning: false });
    
    console.log("🚀 Aggregator Online. Ingesting backlog...");

    await consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: async ({ batch }) => {
            // If the circuit breaker is pulled, skip processing completely
            if (isConsumerPaused) return;

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
                    // Skip malformed bits quietly
                }
            }

            // Log every 50,000 ticks so the console doesn't lag
            if (stats.count >= stats.lastLogCount + 50000) {
                console.log(`📥 Processed: ${stats.count.toLocaleString()} ticks...`);
                stats.lastLogCount = stats.count;
            }
        }
    });
};

run().catch(err => {
    console.error("🔥 CRITICAL SERVER CRASH:", err);
    process.exit(1);
});