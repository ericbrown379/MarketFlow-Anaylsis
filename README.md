# 📊 Market Flow Analysis

## 🧠 Overview

This project builds an end-to-end real-time crypto analytics pipeline using modern data engineering tools.

It ingests live cryptocurrency data from the CoinGecko API, streams it through Kafka, processes it with Spark Structured Streaming, and stores both raw and enriched datasets for downstream analytics.

---

## 🏗️ Architecture

CoinGecko API
	↓
Python Producer
	↓
Kafka Topic
	↓
Spark Structured Streaming
	↓
Data Processing & Enrichment
	↓
Parquet (Data Lake) + PostgreSQL (Serving Layer)

---

## ⚙️ Tech Stack

• Python – Data ingestion (API → Kafka)
• Apache Kafka – Real-time streaming
• Apache Spark – Stream processing & transformations
• Parquet / Delta Lake – Analytical storage
• PostgreSQL – Serving layer for querying
• (Optional) Airflow – Orchestration (removed for local setup)

---

## 🔄 Data Pipeline Flow

1. Fetch cryptocurrency market data from CoinGecko API (every 30 seconds)
2. Produce JSON records into Kafka topic
3. Consume stream using Spark Structured Streaming
4. Parse and transform JSON into structured DataFrame
5. Compute enriched metrics:
   • Price change (1 min, 5 min)
   • Moving averages (SMA, EMA)
   • Rolling volatility (standard deviation)
   • Top 5 gainers and losers
6. Store:
   • Raw + processed data in Parquet (data lake)
   • Aggregated data in PostgreSQL

---

## 📐 Data Schema

coin STRING,
price DOUBLE,
timestamp TIMESTAMP,
market_cap DOUBLE,
volume_24h DOUBLE,
high_24h DOUBLE,
low_24h DOUBLE,
last_updated TIMESTAMP,
ingestion_time TIMESTAMP

### 📈 Derived Metrics
• price_change_1min
• price_change_5min
• SMA (Simple Moving Average)
• EMA (Exponential Moving Average)
• volatility (rolling std deviation)
• top_5_gainers_last_5min
• top_5_losers_last_5min

---

## ⚠️ Technical Constraints

• Runs locally (no distributed cluster)
• Micro-batching every 30 seconds (not true real-time)
• Airflow removed due to local vs Kubernetes mismatch
• Data stored in local filesystem (Parquet)

---

## 🧪 Getting Started

### 1. Prerequisites
• Python 3.10+
• Kafka
• Spark
• Java 11+
• PostgreSQL

---

### 2. Start Kafka
```bash
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
```

---

### 3. Run Producer
```bash
python producer.py
```

### 4. Run Spark Streaming Job
```bash
spark-submit spark_streaming.py
```

### 5. Query Data
• Use PostgreSQL or any BI tool (Power BI, Tableau)
• Or query Parquet files via Spark

---

## 🔥 Key Features

• Real-time streaming pipeline design
• Windowed aggregations & watermarking
• Financial metric computation
• Scalable architecture (cloud-ready)
• Separation of raw vs processed data

---

## 🚀 Future Improvements

• Add Schema Registry (Avro)
• Deploy on GCP (Dataproc + Pub/Sub)
• Add data quality checks (Great Expectations / Soda)
• Partition Parquet data by time
• Build dashboard (Streamlit / Power BI)

---

## 📌 Why This Project Matters

This project demonstrates:
• Real-time data pipeline design
• Streaming + batch hybrid processing
• Data modeling for analytics
• Production-style architecture

---

## 👤 Author

Eric J. Brown
Software Engineer | Data Engineering | Streaming Systems

---

## 💥 (Real talk)

This README is strong enough to:
• Put on your resume
• Talk through in interviews
• Show system design thinking

---

## 📋 Project board

Track tasks, sprints, and issues on the project Kanban board:

[KAN Project Board (Jira)](https://ericbrown379.atlassian.net/jira/software/projects/KAN/boards/1?atlOrigin=eyJpIjoiMjM4OTNjMWNlMzAyNDk2Yzg1NDQ5ZDk3YzRjNWJjM2UiLCJwIjoiaiJ9)

