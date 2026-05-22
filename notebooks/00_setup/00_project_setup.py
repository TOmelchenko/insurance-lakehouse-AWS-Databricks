# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 — Project Setup
# MAGIC Configure project paths, data mode, and schemas.

# COMMAND ----------

PROJECT_NAME = "insurance-lakehouse-databricks-aws"
COMPANY_NAME = "Rheinland Versicherung AG"
DATA_MODE = "test"

S3_BUCKET = "s3://insurance-lakehouse-project"
RAW_BASE_PATH = f"{S3_BUCKET}/raw"
CHECKPOINTS_PATH = f"{S3_BUCKET}/checkpoints"

CATALOG_NAME = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
QUARANTINE_SCHEMA = "quarantine"

print("Project:", PROJECT_NAME)
print("Company:", COMPANY_NAME)
print("Data mode:", DATA_MODE)
print("S3 bucket:", S3_BUCKET)

# COMMAND ----------

# Create catalog, schemas if permissions allow.
#spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{QUARANTINE_SCHEMA}")
