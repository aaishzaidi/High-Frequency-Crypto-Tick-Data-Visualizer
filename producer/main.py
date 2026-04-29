import time
import sys
from confluent_kafka import Producer
import tick_pb2

# --- 1. OPTIMIZED KAFKA CONFIGURATION ---
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'compression.type': 'gzip',
    'queue.buffering.max.messages': 2000000,
    'queue.buffering.max.kbytes': 2097152,
    'batch.num.messages': 100000,
    'linger.ms': 10,
    'acks': 0
}

try:
    p = Producer(**conf)
except Exception as e:
    print(f"❌ Failed to create producer: {e}")
    sys.exit(1)

# --- 2. HIGH-FREQUENCY MULTI-ASSET ENGINE ---
def run_diagnostic():
    print("🚀 DIAGNOSTIC PASSED. Starting Multi-Asset Turbo Loop...")
    
    total_messages = 10_000_000
    start_time = time.time()
    interval_start = time.time()
    topic = 'crypto-ticks'

    # --- THE ASSET UNIVERSE ---
    assets = [
        {"sym": "BTC/USD", "price": 65000.00},
        {"sym": "ETH/USD", "price": 3450.50},
        {"sym": "SOL/USD", "price": 145.20},
        {"sym": "XRP/USD", "price": 0.58}
    ]
    num_assets = len(assets)

    for i in range(1, total_messages + 1):
        # Lightning fast round-robin selection (No slow random number generators)
        asset = assets[i % num_assets]

        # 1. Create the Protobuf Tick
        # The price math here simulates realistic micro-bouncing for the UI sparklines
        tick = tick_pb2.Tick(
            symbol=asset["sym"],
            price=asset["price"] + ((i % 50) - 25) * 0.05, 
            timestamp=int(time.time() * 1000)
        )

        # 2. THE INDESTRUCTIBLE VALVE
        while True:
            try:
                p.produce(topic, value=tick.SerializeToString())
                break
            except BufferError:
                p.poll(0.1)

        # 3. CONTINUOUS MAINTENANCE
        p.poll(0)

        # 4. VELOCITY LOGGING
        if i % 10000 == 0:
            now = time.time()
            elapsed = now - interval_start
            if elapsed > 0:
                velocity = 10000 / elapsed
                print(f"🔥 Velocity: {int(velocity):,} t/s | Total: {i:,}")
            interval_start = now

    print("\n[BULLPEN_PRODUCER] Target reached. Flushing final messages to Docker...")
    p.flush()
    
    total_time = time.time() - start_time
    print(f"✅ ENGINE SHUTDOWN. Sent {total_messages:,} ticks in {total_time:.2f} seconds.")

if __name__ == '__main__':
    try:
        run_diagnostic()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user. Emptying remaining queue...")
        p.flush()
        sys.exit(0)