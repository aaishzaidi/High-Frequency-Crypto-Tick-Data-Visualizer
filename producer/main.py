import time
import random
import sys
from confluent_kafka import Producer
import tick_pb2  # If this fails, it will stop here

# --- PERFORMANCE CONFIG ---
# Change ONLY this line in your 'conf' dictionary:
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'compression.type': 'gzip',  # Changed from 'lz4'
    'queue.buffering.max.messages': 1000000,
    'acks': 0,
    'linger.ms': 5,
}

def run_diagnostic():
    print("⚡ [STAGE 1] Initializing Kafka Producer...", flush=True)
    try:
        p = Producer(conf)
    except Exception as e:
        print(f"❌ KAFKA INIT FAILED: {e}", flush=True)
        return

    print("⚡ [STAGE 2] Testing Protobuf Serialization...", flush=True)
    try:
        tick = tick_pb2.Tick()
        tick.symbol = "BTC"
        tick.price = 50000.0
        tick.timestamp = int(time.time() * 1000)
        payload = tick.SerializeToString()
        print(f"✅ Protobuf OK. Binary size: {len(payload)} bytes", flush=True)
    except Exception as e:
        print(f"❌ PROTOBUF FAILED: {e}", flush=True)
        print("Check if tick_pb2.py exists and is in the producer folder.", flush=True)
        return

    print("⚡ [STAGE 3] Attempting Single-Tick Production...", flush=True)
    try:
        p.produce('crypto-ticks', payload)
        p.flush(timeout=5)
        print("✅ Kafka Delivery OK.", flush=True)
    except Exception as e:
        print(f"❌ DELIVERY FAILED: {e}", flush=True)
        return

    print("\n🚀 DIAGNOSTIC PASSED. Starting Turbo Loop (Ctrl+C to stop)...", flush=True)
    count = 0
    start = time.time()
    
    while True:
        tick.price = random.uniform(49000, 51000)
        tick.timestamp = int(time.time() * 1000)
        p.produce('crypto-ticks', tick.SerializeToString())
        count += 1
        
        if count % 10000 == 0:
            p.poll(0)
            elapsed = time.time() - start
            print(f"🔥 Velocity: {int(count/elapsed):,} t/s | Total: {count:,}", flush=True)

if __name__ == "__main__":
    run_diagnostic()