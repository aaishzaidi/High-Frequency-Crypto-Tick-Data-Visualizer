import json
import time
import numpy as np
from confluent_kafka import Producer

# 1. Configure the Kafka connection to point to our Docker container
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer',
    'linger.ms': 10, # Slight delay to batch messages together for network efficiency
    'batch.num.messages': 10000 
}

producer = Producer(conf)
topic = 'crypto-ticks'

def delivery_report(err, msg):
    """Callback to silently handle delivery success or print errors."""
    if err is not None:
        print(f"Message delivery failed: {err}")

print("🚀 Starting High-Frequency BTC/USD Generator...")

# Starting baseline price for Bitcoin
current_price = 50000.0

try:
    while True:
        # 2. Use NumPy to instantly generate 10,000 random price movements
        batch_size = 10000
        # Create an array of random floats between -2.5 and +2.5
        price_fluctuations = np.random.uniform(-2.5, 2.5, batch_size) 
        
        # Calculate the new prices based on the fluctuations
        prices = current_price + price_fluctuations
        base_timestamp = int(time.time() * 1000)

        # 3. Package and shoot them to Kafka
        for i in range(batch_size):
            tick = {
                "symbol": "BTC/USD",
                "price": round(prices[i], 2),
                "timestamp": base_timestamp + i 
            }
            
            # Convert JSON to bytes and push to the broker
            producer.produce(
                topic, 
                value=json.dumps(tick).encode('utf-8'), 
                callback=delivery_report
            )
        
        # 4. Flush the internal queue to the network
        producer.poll(0)
        print(f"⚡ Pushed {batch_size} ticks to Kafka...")
        
        # We add a tiny 0.1s sleep for testing so it doesn't instantly crash your PC
        # We will remove this later when we stress test the 1M/sec limit
        time.sleep(0.1) 

except KeyboardInterrupt:
    print("\n🛑 Stopping generator...")
finally:
    # Ensure all remaining messages in memory are sent before shutting down
    producer.flush()
    print("Clean shutdown complete.")