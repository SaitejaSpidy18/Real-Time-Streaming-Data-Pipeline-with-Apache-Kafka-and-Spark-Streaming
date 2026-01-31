from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, approx_count_distinct,
    to_timestamp, to_date, current_timestamp, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DB_URL = os.getenv("DB_URL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

spark = (
    SparkSession.builder
    .appName("RealTimeUserActivityPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

event_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True),
])

raw_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "user_activity")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw_kafka
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time_ts", to_timestamp("event_time"))
    .filter(col("event_time_ts").isNotNull())
)

events = parsed.withWatermark("event_time_ts", "2 minutes")

# 1) 1-min tumbling window page_view counts
page_views = (
    events.filter(col("event_type") == "page_view")
    .groupBy(
        window(col("event_time_ts"), "1 minute"),
        col("page_url")
    )
    .agg(count("*").alias("view_count"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("page_url"),
        col("view_count")
    )
)

def upsert_page_view(batch_df, batch_id):
    (
        batch_df
        .write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "page_view_counts")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

page_view_query = (
    page_views.writeStream
    .foreachBatch(upsert_page_view)
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark/checkpoints/page_view_counts")
    .start()
)

# 2) 5-min sliding window active users
active_users = (
    events
    .groupBy(
        window(col("event_time_ts"), "5 minutes", "1 minute")
    )
    .agg(approx_count_distinct("user_id").alias("active_user_count"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("active_user_count")
    )
)

def upsert_active_users(batch_df, batch_id):
    (
        batch_df
        .write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "active_users")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

active_users_query = (
    active_users.writeStream
    .foreachBatch(upsert_active_users)
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark/checkpoints/active_users")
    .start()
)

# 3) Data lake sink
lake_events = events.withColumn("event_date", to_date("event_time_ts"))

lake_query = (
    lake_events.writeStream
    .format("parquet")
    .option("path", "/opt/spark/data/lake")
    .option("checkpointLocation", "/opt/spark/checkpoints/lake")
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

# 4) Enriched Kafka sink
enriched = events.select(
    "event_time",
    "user_id",
    "page_url",
    "event_type",
    current_timestamp().alias("processing_time")
)

enriched_json = enriched.select(
    to_json(
        struct(
            col("event_time"),
            col("user_id"),
            col("page_url"),
            col("event_type"),
            col("processing_time").cast("string")
        )
    ).alias("value")
)

enriched_query = (
    enriched_json.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", "enriched_activity")
    .option("checkpointLocation", "/opt/spark/checkpoints/enriched_activity")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
