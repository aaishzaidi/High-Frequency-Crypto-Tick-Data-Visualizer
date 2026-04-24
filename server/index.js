const { Kafka } = require('kafkajs');

// 1. Configure the Kafka connection
const kafka = new Kafka({
  clientId: 'node-relay-server',
  brokers: ['127.0.0.1:9092']
});

// 2. Create a consumer and assign it to a "group"
const consumer = kafka.consumer({ groupId: 'crypto-group' });

let messageCount = 0;

const run = async () => {
  try {
    // 3. Connect to the Kafka broker
    await consumer.connect();
    console.log('🔌 Connected to Kafka successfully.');

    // 4. Subscribe to the exact topic the Python script is pushing to
    await consumer.subscribe({ topic: 'crypto-ticks', fromBeginning: false });
    console.log('📡 Subscribed to [crypto-ticks]. Waiting for data stream...');

    // 5. Start listening for messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        messageCount++;
        
        // Only print every 10,000th message so we don't crash the terminal!
        if (messageCount % 10000 === 0) {
           const tick = JSON.parse(message.value.toString());
           console.log(`📦 Caught 10,000 ticks! Latest Price: $${tick.price} at ${tick.timestamp}`);
        }
      },
    });
  } catch (error) {
    console.error(`❌ Error connecting to Kafka: ${error}`);
  }
};

run().catch(console.error);