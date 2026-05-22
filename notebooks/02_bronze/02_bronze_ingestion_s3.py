# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Ingestion: Customers

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

S3_BUCKET = "s3a://insurance-lakehouse-project"
RAW_BASE_PATH = f"{S3_BUCKET}/raw"
CATALOG_NAME = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"

ingest_run_id = str(uuid.uuid4())
print("Run id:", ingest_run_id)

datasets = ["customers", "policies", "claims", "payments", "agents", "fraud_indicators"]

results = []

for name in datasets:
    raw_path = f"{RAW_BASE_PATH}/{name}"
    bronze_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_{name}"

    print(f"\nIngesting {name}...")
    print(f"Raw path: {raw_path}")
    print(f"Bronze table: {bronze_table}")

    raw_df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

    bronze_df = (
        raw_df
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("source_file_name", F.input_file_name())
    )

    bronze_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

    raw_count = raw_df.count()
    bronze_count = spark.table(bronze_table).count()
    status = "PASS" if raw_count == bronze_count else "FAIL"

    print(f"{name} - raw: {raw_count}, bronze: {bronze_count}, status: {status}")
    results.append((name, raw_path, bronze_table, raw_count, bronze_count, status))

summary = spark.createDataFrame(results, ["dataset", "raw_path", "bronze_table", "raw_count", "bronze_count", "status"])
display(summary)
