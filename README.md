# 🧠 Kafka Pipeline for Stock Market Anomalies

This repository is an improvement over the [Financial Anomalies Kafka project](https://github.com/aghersidev/financial-anomalies-kafka) and contains an updated set of Java applications. These applications work together as a pipeline to process stock market data in the form of candles and send found anomalies to the output topic.

### 🧩 Pipeline Components

1. **📤 Kafka Producer**: Reads data from Google Drive and produces raw messages to the Kafka topic `input-data`. (You can replace it with your own producer.)
2. **🧮 Augment Data**: Adds derived features like body and shadow size.
3. **🌲 Isolation Forest**: Uses Smile's Isolation Forest to detect anomalies in stock market candles.

✅ Uses Avro for serialization and multithreading for asynchronous training.

---

### 📈 Performance Metrics

| 📊 Metric                   | 🔢 Value                       | 💬 Notes                      |
|----------------------------|-------------------------------|-------------------------------|
| 🗓️ Daily throughput         | **1.750 billion records/day** | High ingestion rate           |
| 📦 Bytes processed / record| **65.0**                      | 51% improvement               |
| 🧾 Bytes produced / record | **1.54**                      | 52% improvement               |
| ⚠️ Anomalies per million   | **145,268**                   | 1.45% of total records        |

---

### 📝 Notes

- 🧱 Legacy system has significantly higher byte cost per record  
- 📉 New system isolates more anomalies while using fewer bytes  
- 🧬 Likely byte-size savings due to Avro serialization  

---

## 🧰 How to Clone the Repository

```bash
mkdir kafka-pipeline-v2
git clone https://github.com/aghersidev/financial-anomalies-kafka-v2.git
cd kafka-pipeline-v2
```

---

## 🛠️ Setting Up Kafka (Using Kraft)

This project uses Kafka in **Kraft mode** (no Zookeeper).

### 🪟 If you're on Windows:

1. **📥 Download Kafka**  
   Follow the [Kafka Quickstart Guide](https://kafka.apache.org/quickstart).

2. **🚀 Start Kafka in Kraft Mode**  
   ```bash
   bin[/windows]/kafka-server-start.sh config/kraft-server.properties
   ```

3. **📡 Create Topics**
   ```bash
   bin/kafka-topics.sh --create --topic input-data --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic augmented --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic anomalies --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
   ```

---

## ☕ How to Compile and Run the Java Projects

Each Java application lives in its own folder.

### 🔧 Build

```bash
cd kafka-producer  # or kafka-consumer, etc.
mvn clean install
```

### ▶️ Run (in order)

1. **📤 Producer**: Sends data from Google Drive to Kafka  
2. **🧮 Augment Data**: Enhances and transforms raw data  
3. **🌲 Isolation Forest**: Detects anomalies and emits alerts  

Ensure Kafka is running before launching the producer.

---

## 🙌 Credits

- **Kafka**: [Apache Kafka](https://kafka.apache.org/)
- **Smile**: [Smile - Statistical Machine Intelligence](https://haifengl.github.io/smile/)
- **RabbitMQ**: [RabbitMQ](https://www.rabbitmq.com/)

---

## ⚖️ License

MIT License — see [LICENSE](LICENSE) for details.
