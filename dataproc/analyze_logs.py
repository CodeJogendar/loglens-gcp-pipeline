"""PySpark (Dataproc): clean raw API logs, then compute service health, hourly incident
detection and failing-endpoint tables. Output: Parquet in GCS."""
import sys
from pyspark.sql import SparkSession, functions as F

bucket = sys.argv[1]
spark = SparkSession.builder.appName("loglens-analysis").getOrCreate()
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

raw = (spark.read.option("header", True).option("inferSchema", True)
       .csv(f"gs://{bucket}/raw/logs_historical.csv"))
print("RAW ROWS:", raw.count())

clean = (raw
    .dropDuplicates(["log_id"])
    .withColumn("latency_ms", F.col("latency_ms").cast("int"))
    .filter(F.col("latency_ms") >= 0)
    .withColumn("http_status", F.col("http_status").cast("int"))
    .withColumn("region", F.when(F.col("region").isNull() | (F.trim("region") == ""), "unknown")
                           .otherwise(F.lower(F.trim("region"))))
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("severity", F.upper(F.trim("severity")))
    .select("log_id", "event_ts", "service", "region", "endpoint",
            "http_status", "latency_ms", "severity", "message"))
print("CLEAN ROWS:", clean.count())
clean.write.mode("overwrite").parquet(f"gs://{bucket}/processed/logs_clean/")

is_5xx = (F.col("http_status") >= 500).cast("int")
is_4xx = ((F.col("http_status") >= 400) & (F.col("http_status") < 500)).cast("int")

health = (clean.groupBy("service")
    .agg(F.count("*").alias("requests"),
         F.sum(is_5xx).alias("errors_5xx"),
         F.sum(is_4xx).alias("errors_4xx"),
         F.round(F.avg("latency_ms"), 1).alias("avg_latency_ms"),
         F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
         F.max("latency_ms").alias("max_latency_ms"))
    .withColumn("error_rate_5xx", F.round(F.col("errors_5xx") / F.col("requests"), 3)))
health.write.mode("overwrite").parquet(f"gs://{bucket}/processed/service_health/")

hourly = (clean.withColumn("hour_start", F.date_trunc("hour", "event_ts"))
    .groupBy("service", "hour_start")
    .agg(F.count("*").alias("requests"),
         F.sum(is_5xx).alias("errors_5xx"),
         F.sum((F.col("http_status") == 403).cast("int")).alias("forbidden_403"),
         F.sum((F.col("http_status") == 429).cast("int")).alias("rate_limited_429"),
         F.sum((F.col("http_status") == 504).cast("int")).alias("timeouts_504"),
         F.sum((F.col("latency_ms") >= 2000).cast("int")).alias("slow_requests"),
         F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"))
    .withColumn("incident_type",
        F.when(F.col("forbidden_403") >= 3, "PERMISSION_DENIED")
         .when(F.col("rate_limited_429") >= 3, "RATE_LIMITING")
         .when(F.col("timeouts_504") >= 3, "UPSTREAM_TIMEOUT")
         .when(F.col("errors_5xx") >= 3, "5XX_BURST")
         .when(F.col("slow_requests") >= 3, "LATENCY_SPIKE")
         .otherwise("NORMAL")))
hourly.write.mode("overwrite").parquet(f"gs://{bucket}/processed/service_hourly/")
print("INCIDENT BUCKETS:")
hourly.filter(F.col("incident_type") != "NORMAL").groupBy("incident_type").count().show()

fails = (clean.filter(F.col("http_status") >= 400)
    .groupBy("service", "endpoint", "http_status").agg(F.count("*").alias("failures")))
fails.write.mode("overwrite").parquet(f"gs://{bucket}/processed/endpoint_failures/")
spark.stop()
