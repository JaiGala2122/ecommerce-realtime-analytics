# 🛒 Real-Time E-Commerce Analytics  
**End-to-End Streaming Pipeline using Kafka, Spark & Postgres**

This repository demonstrates a **real-time data processing architecture** using the *UCI Online Retail* dataset.  
It focuses on the technical implementation, while conclusions and business insights are presented separately on the website (`docs/index.html`).

---

## ⚙️ Architecture Overview
```
Excel (raw sales data)
   ↓
prepare_offline_sample.py → CSV + JSONL
   ↓
stream_simulator.py → Kafka Topic
   ↓
spark_job.py → Streaming Aggregation
   ↓
Postgres / Parquet Storage
```

---



## 🚀 Quickstart Guide

### 1️⃣ Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2️⃣ Launch Local Infrastructure (optional)
```bash
docker compose up -d
```
This starts:
- **Zookeeper** on port 2181  
- **Kafka Broker** on port 9092  
- **Postgres** on port 5432  

---

### 3️⃣ Prepare Data
Convert the raw Excel dataset into clean CSV and JSONL for streaming.
```bash
python src/prepare_offline_sample.py
```
Outputs:
- `data/online_retail_clean.csv`
- `data/events.jsonl`

---

### 4️⃣ Simulate Real-Time Stream
Publish transactions to the Kafka topic defined in `config.yaml`.
```bash
python src/stream_simulator.py
```

---

### 5️⃣ Run Spark Streaming Job
Aggregate transactions (per-minute metrics by country) and save to Parquet.
```bash
python src/spark_job.py
```

---

## 🧠 Key Features
- Uses **real transactional data** (UCI Online Retail)
- Fully configurable via YAML (`src/config.yaml`)
- **Kafka → Spark → Postgres/Parquet** pipeline
- Modular, production-style code organization
- Reproducible with **Docker Compose**

---

## 🧩 Tech Stack
- **Apache Kafka** – event streaming  
- **Apache Spark** – structured streaming & aggregation  
- **PostgreSQL / Parquet** – storage layer  
- **Python (pandas, kafka-python, pyspark)** – ETL scripting  
- **Docker Compose** – infrastructure orchestration  
