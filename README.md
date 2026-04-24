High-Frequency Crypto Tick Data Visualizer
  This project is a full-stack, real-time data pipeline capable of processing and visualizing tens of thousands of simulated cryptocurrency price "ticks" per second. It demonstrates the use of distributed systems to handle high-throughput data without crashing the client-side interface.

The Technical Architecture
  To present this effectively, you must be able to explain the flow of data through these four distinct layers:
  
  The Producer (Python): Generates high-frequency mock data (10,000+ ticks/sec) and pushes it to a Kafka topic. This mimics a live exchange feed.
  
  The Broker (Apache Kafka/Docker): Acts as the central nervous system. It decouples the data generation from the consumption, ensuring that if the frontend slows down, the data isn't lost.
  
  The Relay Server (Node.js): Connects to Kafka as a consumer. Its critical role is Batching. It collects thousands of individual messages and bundles them into 100ms "packets" to be sent via WebSockets.
  
  The Frontend (React & TradingView): Uses high-performance canvas rendering (Lightweight Charts) to visualize the data.

Core Concepts to Master for Presentation
  1. Backpressure and Decoupling
  Explain why we didn't connect Python directly to React. In high-frequency systems, if the producer is faster than the consumer, the system crashes. Kafka acts as a "buffer" (Backpressure management), allowing the components to work at their own speeds.
  
  2. The "Batching" Strategy
  Be ready to explain why we don't send every tick individually to the browser. Sending 10,000 WebSocket messages per second would freeze the browser's main thread. By batching data into 100ms intervals, we maintain the "real-time" feel while significantly reducing CPU overhead.
  
  3. Docker Infrastructure Lessons
  Mention the shift from Kafka :latest to version 7.4.0. This demonstrates an awareness of the transition from Zookeeper-based Kafka to KRaft (Kafka Raft) and the importance of environment-specific configurations like KAFKA_ADVERTISED_LISTENERS.
  
  4. Scalability
  If asked how this scales, discuss adding more partitions to the Kafka topic or deploying multiple instances of the Node.js relay server to handle more frontend clients.

Presentation Tips
  The "Moment of Truth" Demo: Keep three terminals open alongside the browser. Start them in order: Docker -> Node.js -> Python -> React. Seeing the data "flow" through the terminals into the chart is the most impactful part of the demo.
  
  Performance Metrics: Note that this setup can comfortably handle 100,000 ticks per second on a standard local machine, which is far beyond the capacity of traditional REST-API-based dashboards.
