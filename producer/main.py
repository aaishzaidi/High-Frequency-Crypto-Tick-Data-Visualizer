import time
import random
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer
import tick_pb2  # The dictionary we just generated

# --- CONFIGURATION FOR 1M TICKS/SEC ---
conf = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 2000000, # Hold up to 2M messages in RAM
    'batch.size': 262144,                    # 256KB batches
    'linger.ms': 5,                          # Wait 5ms to bundle messages
    'compression.type': 'lz4',               # Fastest compression for CPU
    'acks': 0                                # 0 = Fire and forget (Fastest)
}

producer = Producer(conf)

SYMBOLS = [f"COIN_{i}" for i in range(100)] # 100 assets
THREADS = 8 # Matches modern CPU cores

def stream_market_sector(thread_id, symbols_subset):
    count = 0
    start_time = time.time()
    
    while True:
        for symbol in symbols_subset:
            # 1. Create the binary object (Protobuf)
            tick = tick_pb2.Tick()
            tick.symbol = symbol
            tick.price = random.uniform(100, 60000)
            tick.timestamp = int(time.time() * 1000)
            
            # 2. Serialize to Bits
            payload = tick.SerializeToString()
            
            # 3. Fire into Kafka
            producer.produce('crypto-ticks', payload)
            count += 1
            
            # Periodic flush to clear internal queue
            if count % 50000 == 0:
                producer.poll(0) # Non-blocking poll
                elapsed = time.time() - start_time
                velocity = int(count / elapsed)
                print(f"🚀 [Thread {thread_id}] Velocity: {velocity:,} ticks/sec")

# Distribute 100 symbols across 8 threads
chunk = len(SYMBOLS) // THREADS
print(f"🔥 Starting Ingestion Engine: Target 1,000,000 ticks/sec...")

with ThreadPoolExecutor(max_workers=THREADS) as executor:
    for i in range(THREADS):
        subset = SYMBOLS[i*chunk : (i+1)*chunk]
        executor.submit(stream_market_sector, i, subset)