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


---

## 📊 How SMA (Simple Moving Average) is calculated

Simple Moving Average (SMA) over a window of size k is the arithmetic mean of the last k prices:

SMA_k = (1 / k) * sum_{i=n-k+1..n} p_i

Example (k = 3): prices = [100, 105, 110]

SMA_3 = (100 + 105 + 110) / 3 = 315 / 3 = 105

In this project we compute the SMA per coin using a row-based window over the last k rows (micro-batch aware). In PySpark you can compute a 5-period SMA per coin with something like:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col

coin_window = Window.partitionBy('id').orderBy('ingestion_time').rowsBetween(-4, 0)
df = df.withColumn('SMA_5', avg(col('price')).over(coin_window))
```

Notes:
- rowsBetween(-4, 0) takes the current row and the previous 4 rows (total 5) per coin.
- Use a timestamp-based window or watermark for time-aware aggregations when late data is expected.


---

## 📊 How EMA (Exponential Moving Average) is calculated

The Exponential Moving Average (EMA) gives more weight to recent prices. The true EMA is recursive:

EMA_t = α * p_t + (1 - α) * EMA_{t-1}

where α is the smoothing factor (0 < α ≤ 1). This requires keeping the previous EMA value (stateful / recursive).

Practical shortcut (streaming-friendly): approximate an EMA with a small weighted rolling window (e.g., 3 points), giving higher weight to the most recent value. This is not a true EMA, but is simple and often effective for short windows.

Example (3-point weighted shortcut): current price = p_t, previous prices p_{t-1}, p_{t-2} with weights [0.6, 0.3, 0.1]:

EMA_3_approx = (0.6 * p_t + 0.3 * p_{t-1} + 0.1 * p_{t-2}) / (0.6 + 0.3 + 0.1)

PySpark (approximate 3-point EMA using lag in a window):

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

coin_window = Window.partitionBy('id').orderBy('ingestion_time')

# create lag columns for previous 1 and 2 prices
df = df.withColumn('p0', col('price')) \
      .withColumn('p1', lag('price', 1).over(coin_window)) \
      .withColumn('p2', lag('price', 2).over(coin_window))

# weighted combination (handles nulls by leaving EMA null until enough history exists)
df = df.withColumn('EMA_3_approx', (
   col('p0') * 0.6 + col('p1') * 0.3 + col('p2') * 0.1
))

# if you want to normalize by total weights (useful when some lags are null):
df = df.withColumn('EMA_3_approx', (
   (col('p0') * 0.6 + col('p1') * 0.3 + col('p2') * 0.1) / (0.6 + 0.3 + 0.1)
))
```

Notes:
- The snippet above is a practical shortcut and not a mathematically exact EMA.
- For a true streaming EMA you need to maintain previous EMA state per key — use stateful processing (mapGroupsWithState) or store the last EMA externally and use it when computing the next value.
- Choose α (smoothing factor) according to how quickly you want the EMA to react (larger α = more weight on recent prices).

---

## 📊 Volatility (rolling standard deviation)

Volatility measures how much a price fluctuates over time. High volatility indicates large, frequent price swings (riskier / less predictable). Low volatility indicates more stable prices.

One common measure is the standard deviation (σ) of prices over a window of n observations:

σ = sqrt( (1/n) * sum_{i=1..n} (x_i - μ)^2 )

In PySpark we compute a rolling volatility per coin using a windowed stddev over the last k rows (e.g., 5):

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import stddev, col

coin_window = Window.partitionBy('id').orderBy('ingestion_time').rowsBetween(-4, 0)
df = df.withColumn('volatility_5', stddev(col('price')).over(coin_window))
```

Notes:
- Like the SMA, rowsBetween(-4, 0) uses the current row and the previous 4 rows (total 5) per coin.
- The result will be NULL until enough history exists for the window.
- For time-aware volatility with late-arriving data, use time windows and watermarking rather than purely row-based windows.


