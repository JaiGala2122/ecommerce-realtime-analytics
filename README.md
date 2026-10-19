# Real-Time E-Commerce Analytics - Streaming ETL Project

A streaming data pipeline that replays a year of online-retail sales as a live feed and keeps a sales table up to date in PostgreSQL.

**Full write-up, results and charts:** [galajainam.com](https://www.galajainam.com/projects/realtime-analytics/index.html)

## 🎯 Project Overview

This project shows data engineering skills through:
- Cleaning raw sales data with Python
- Streaming events in real time with Kafka and Spark
- Saving every event to Parquet and hourly sales to Postgres
- Checking that no data was lost along the way

## 📦 Dataset

[UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) - 541,909 sales lines from a UK online gift shop, Dec 2010 to Dec 2011.

## ⚙️ How It Works

1. `prepare_data.py` - cleans the Excel file and saves it as events
2. `stream_simulator.py` - sends the events one by one, like a live store
3. `spark_job.py` - reads the stream, then:
   - saves every event to Parquet
   - updates sales per country per hour in Postgres

## 🛠️ Skills Demonstrated

- Data cleaning with pandas
- Spark Structured Streaming
- Kafka event streaming
- PostgreSQL upserts
- Docker Compose setup
- Data checks (row counts and revenue match the source)

## 🚀 Quick Start

```bash
pip install -r requirements.txt
python src/prepare_data.py
python src/spark_job.py          # terminal 1
python src/stream_simulator.py   # terminal 2
```

Needs Python 3.10+, Java 17+ and a Postgres database named `ecom`.
To use Kafka, run `docker compose up -d` and set `source: "kafka"` in `src/config.yaml`.

## 📁 Files

```
src/            Python code and config
sql/schema.sql  Postgres table
data/           Dataset and run metrics
```
