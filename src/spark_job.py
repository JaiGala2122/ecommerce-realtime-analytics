
import os, yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, avg as _avg, count as _count, expr

conf_path = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(conf_path, "r") as f:
    CONF = yaml.safe_load(f)

spark = (
    SparkSession.builder
    .appName(CONF["spark"]["app_name"])
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", ",".join(CONF["kafka"]["bootstrap_servers"]))
         .option("subscribe", CONF["spark"]["kafka_topic"])
         .option("startingOffsets", "latest")
         .load()
)

# Parse JSON values
from pyspark.sql.types import *
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("is_return", IntegerType(), True),
])

parsed = (
    df.selectExpr("CAST(value AS STRING) as json")
      .selectExpr("from_json(json, '{}') as data".format(schema.simpleString()))
      .select("data.*")
      .withColumn("event_time", expr("to_timestamp(event_time)"))
)

# 1-minute by country metrics
metrics = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute").alias("w"),
        col("country")
    )
    .agg(
        _count("*").alias("transaction_count"),
        _sum("revenue").alias("total_revenue"),
        _avg("revenue").alias("avg_revenue"),
    )
    .selectExpr(
        "w.start as window_start",
        "w.end as window_end",
        "country",
        "transaction_count",
        "total_revenue",
        "avg_revenue"
    )
)

# Write to Parquet (dev-friendly); also optional JDBC to Postgres
parquet_dir = CONF["storage"]["parquet_dir"]
query = (
    metrics.writeStream
           .format("parquet")
           .option("path", parquet_dir)
           .option("checkpointLocation", CONF["spark"]["checkpoint_dir"])
           .outputMode("append")
           .start()
)

print(f"Writing streaming aggregates to {parquet_dir} ...")
query.awaitTermination()
