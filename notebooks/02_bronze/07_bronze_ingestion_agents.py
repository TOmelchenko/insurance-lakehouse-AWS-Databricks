# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Bronze Ingestion: Agents

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

S3_BUCKET = "s3://databricks-sfjvkaqmnimrsa4tfyg8cs-cloud-storage-bucket"
RAW_BASE_PATH = f"{S3_BUCKET}/raw"
CATALOG_NAME = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"

DATASET_NAME = "agents"
RAW_PATH = f"{RAW_BASE_PATH}/agents"
BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_agents"
ingest_run_id = str(uuid.uuid4())

print("Raw path:", RAW_PATH)
print("Bronze table:", BRONZE_TABLE)
print("Run id:", ingest_run_id)

raw_df = spark.read.option("header", True).option("inferSchema", True).csv(RAW_PATH)

bronze_df = (
    raw_df
    .withColumn("ingest_timestamp", F.current_timestamp())
    .withColumn("ingest_run_id", F.lit(ingest_run_id))
    .withColumn("source_file_name", F.input_file_name())
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)

raw_count = raw_df.count()
bronze_count = spark.table(BRONZE_TABLE).count()

print("Raw count:", raw_count)
print("Bronze count:", bronze_count)
print("Status:", "PASS" if raw_count == bronze_count else "FAIL")

display(spark.table(BRONZE_TABLE).limit(10))
