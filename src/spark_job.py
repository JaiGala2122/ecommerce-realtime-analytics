"""Spark Structured Streaming job: stream -> bronze Parquet + hourly sales in Postgres.

Two queries share one parsed stream:
  bronze  every cleaned event appended to Parquet (the replayable history)
  gold    revenue, orders and returns per country per hour, upserted into Postgres

Every gold micro-batch logs its freshness: the time from the newest event's
ingest_time to the moment its row is committed in Postgres.

  python src/spark_job.py                  # runs until the stream is idle for 30s
  python src/spark_job.py --idle-stop 0    # run forever
"""
import argparse
import csv
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)

from common import ROOT, load_config, path

CONF = load_config()
PG = CONF["storage"]["postgres"]

SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("invoice_no", StringType()),
    StructField("stock_code", StringType()),
    StructField("description", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("customer_id", LongType()),
    StructField("country", StringType()),
    StructField("revenue", DoubleType()),
    StructField("is_return", BooleanType()),
    StructField("is_cancellation", BooleanType()),
    StructField("ingest_time", StringType()),
])

UPSERT = f"""
INSERT INTO {PG['table']}
  (window_start, window_end, country, order_lines, invoices, revenue, returns_value,
   last_ingest_time, updated_at)
VALUES %s
ON CONFLICT (window_start, country) DO UPDATE SET
  window_end = EXCLUDED.window_end,
  order_lines = EXCLUDED.order_lines,
  invoices = EXCLUDED.invoices,
  revenue = EXCLUDED.revenue,
  returns_value = EXCLUDED.returns_value,
  last_ingest_time = EXCLUDED.last_ingest_time,
  updated_at = now()
"""


def build_spark() -> SparkSession:
    b = (SparkSession.builder.appName(CONF["spark"]["app_name"])
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.streaming.numRecentProgressUpdates", "100000"))
    if CONF["source"] == "kafka":
        b = b.config("spark.jars.packages", CONF["spark"]["kafka_package"])
    return b.getOrCreate()


def read_stream(spark):
    if CONF["source"] == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", ",".join(CONF["kafka"]["bootstrap_servers"]))
               .option("subscribe", CONF["kafka"]["topic"])
               .option("startingOffsets", "earliest")
               .load())
        parsed = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
    else:
        landing = path(CONF["stream"]["landing_dir"])
        landing.mkdir(parents=True, exist_ok=True)
        parsed = spark.readStream.schema(SCHEMA).json(str(landing))
    return (parsed
            .withColumn("event_time", F.to_timestamp("event_time"))
            .withColumn("ingest_time", F.to_timestamp("ingest_time"))
            .filter(F.col("event_time").isNotNull()))


def ensure_table() -> None:
    sql = (ROOT / "sql" / "schema.sql").read_text()
    with psycopg2.connect(**_pg()) as conn, conn.cursor() as cur:
        cur.execute(sql)


def _pg() -> dict:
    return {k: PG[k] for k in ("host", "port", "dbname", "user", "password")}


def make_writer(metrics_path):
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    new = not metrics_path.exists()
    fh = metrics_path.open("a", newline="")
    log = csv.writer(fh)
    if new:
        log.writerow(["batch_id", "rows_upserted", "write_ms", "freshness_s", "logged_at"])

    def write(batch_df, batch_id):
        t0 = time.perf_counter()
        rows = batch_df.collect()            # small: one row per country-hour touched
        if not rows:
            return
        values = [(r.window_start, r.window_end, r.country, r.order_lines, r.invoices,
                   round(r.revenue, 2), round(r.returns_value, 2), r.last_ingest_time,
                   datetime.now(timezone.utc)) for r in rows]
        with psycopg2.connect(**_pg()) as conn, conn.cursor() as cur:
            execute_values(cur, UPSERT, values, page_size=1000)
        committed = datetime.now(timezone.utc).replace(tzinfo=None)
        newest = max(r.last_ingest_time for r in rows)
        log.writerow([batch_id, len(rows), round((time.perf_counter() - t0) * 1000),
                      round((committed - newest).total_seconds(), 2), committed.isoformat()])
        fh.flush()
    return write


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--idle-stop", type=int, default=30,
                    help="stop after N seconds with no new input (0 = never)")
    args = ap.parse_args()

    ensure_table()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    events = read_stream(spark)
    ckpt = path(CONF["spark"]["checkpoint_dir"])
    trigger = f"{CONF['spark']['trigger_seconds']} seconds"

    bronze = (events.withColumn("event_date", F.to_date("event_time"))
              .writeStream.format("parquet")
              .option("path", str(path(CONF["storage"]["bronze_dir"])))
              .option("checkpointLocation", str(ckpt / "bronze"))
              .partitionBy("event_date")
              .trigger(processingTime=trigger)
              .start())

    w = F.window("event_time", CONF["spark"]["window"])
    gold_df = (events.withWatermark("event_time", CONF["spark"]["watermark"])
               .groupBy(w.alias("w"), "country")
               .agg(F.count("*").alias("order_lines"),
                    F.approx_count_distinct("invoice_no").alias("invoices"),
                    F.sum(F.when(~F.col("is_return"), F.col("revenue")).otherwise(0.0)).alias("revenue"),
                    F.sum(F.when(F.col("is_return"), -F.col("revenue")).otherwise(0.0)).alias("returns_value"),
                    F.max("ingest_time").alias("last_ingest_time"))
               .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                       "country", "order_lines", "invoices", "revenue", "returns_value",
                       "last_ingest_time"))

    metrics_dir = path(CONF["storage"]["metrics_dir"])
    gold = (gold_df.writeStream.outputMode("update")
            .foreachBatch(make_writer(metrics_dir / "gold_batches.csv"))
            .option("checkpointLocation", str(ckpt / "gold"))
            .trigger(processingTime=trigger)
            .start())

    print(f"Streaming from {CONF['source']} -> Parquet + Postgres table {PG['table']} ...")
    last_input = time.time()
    try:
        while gold.isActive:
            time.sleep(2)
            p = gold.lastProgress
            if p and p.get("numInputRows", 0) > 0:
                last_input = time.time()
            if args.idle_stop and time.time() - last_input > args.idle_stop:
                print(f"No new input for {args.idle_stop}s, stopping.")
                break
    finally:
        progress = [p for p in gold.recentProgress if p.get("numInputRows", 0) > 0]
        bronze.stop()
        gold.stop()
        with (metrics_dir / "spark_progress.csv").open("w", newline="") as fh:
            out = csv.writer(fh)
            out.writerow(["batch_id", "input_rows", "processed_rows_per_s", "trigger_ms"])
            for p in progress:
                out.writerow([p["batchId"], p["numInputRows"],
                              round(p.get("processedRowsPerSecond") or 0),
                              p["durationMs"].get("triggerExecution")])
        spark.stop()


if __name__ == "__main__":
    main()
