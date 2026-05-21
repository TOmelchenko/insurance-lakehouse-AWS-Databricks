# Databricks notebook source
# MAGIC %md
# MAGIC # 22 — Day 3 Gold Validation
# MAGIC
# MAGIC Week 11 Day 3 — Gold Analytics, Insurance KPIs, AI-Ready Features, and Performance.
# MAGIC
# MAGIC Replace catalog/schema names if your environment uses different names.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "insurance_lakehouse"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

def t(schema, table):
    return f"{CATALOG}.{schema}.{table}"

# COMMAND ----------


gold_tables = [
    ("gold_claims_overview", []),
    ("gold_policy_performance", []),
    ("gold_customer_risk_profile", ["customer_id"]),
    ("gold_claims_payment_summary", ["claim_id"]),
    ("gold_fraud_risk_summary", []),
    ("gold_agent_performance", ["agent_id"]),
    ("gold_claim_fraud_features", ["claim_id"]),
]
results = []
for table, grain_cols in gold_tables:
    df = spark.table(t(GOLD_SCHEMA, table))
    row_count = df.count()
    duplicate_count = None
    if grain_cols:
        duplicate_count = df.groupBy(*grain_cols).count().filter(F.col("count") > 1).count()
    results.append((table, row_count, ",".join(grain_cols), duplicate_count))
validation_df = spark.createDataFrame(results, ["table_name", "row_count", "grain_columns", "duplicate_grain_count"])
display(validation_df)
validation_df.write.mode("overwrite").format("delta").saveAsTable(t(GOLD_SCHEMA, "gold_day3_validation_summary"))

