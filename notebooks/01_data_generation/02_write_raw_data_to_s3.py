# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 02 — Write Raw Data to S3

# COMMAND ----------

#S3_BUCKET = "s3://insurance-lakehouse-project-tomelchenko"
S3_BUCKET = "s3://databricks-sfjvkaqmnimrsa4tfyg8cs-cloud-storage-bucket"
RAW_BASE_PATH = f"{S3_BUCKET}/raw"
DATA_MODE = "small"
PARTITIONS = 8 if DATA_MODE == "small" else 128

datasets = {
    "customers": spark.table("tmp_customers"),
    "policies": spark.table("tmp_policies"),
    "claims": spark.table("tmp_claims"),
    "payments": spark.table("tmp_payments"),
    "agents": spark.table("tmp_agents"),
    "fraud_indicators": spark.table("tmp_fraud_indicators"),
}

for name, df in datasets.items():
    target_path = f"{RAW_BASE_PATH}/{name}"
    print("Writing", name, "to", target_path)
    df.repartition(PARTITIONS).write.mode("overwrite").option("header", True).csv(target_path)

raw_counts = []
for name in datasets:
    count_value = spark.read.option("header", True).csv(f"{RAW_BASE_PATH}/{name}").count()
    raw_counts.append((name, count_value))
display(spark.createDataFrame(raw_counts, ["dataset", "raw_count"]))
