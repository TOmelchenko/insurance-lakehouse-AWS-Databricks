# Databricks notebook source
# MAGIC %md
# MAGIC # 23 — Day 3 Performance Optimization
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


feature_df = spark.table(t(GOLD_SCHEMA, "gold_claim_fraud_features"))
feature_df.explain(True)
row_count = feature_df.count()
print("gold_claim_fraud_features row count:", row_count)
try:
    spark.sql(f"OPTIMIZE {t(GOLD_SCHEMA, 'gold_claim_fraud_features')}")
    print("OPTIMIZE completed.")
except Exception as e:
    print("OPTIMIZE was not available or failed in this environment.")
    print(str(e)[:500])

