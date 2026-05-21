# Databricks notebook source
# MAGIC %md
# MAGIC # 16 — Gold Policy Performance
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

# select only required columns before joins - performance rule
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "customer_id", "policy_type", "policy_status",
    "sales_channel", "premium_amount", "coverage_amount"
)
customers = spark.table(t(SILVER_SCHEMA, "silver_customers")).select(
    "customer_id", "bundesland"
)

policies_enriched = policies.join(customers, on="customer_id", how="left")

gold_policy_performance = (
    policies_enriched
    .groupBy("policy_type", "policy_status", "sales_channel", "bundesland")
    .agg(
        F.count("*").alias("total_policies"),
        F.sum(F.when(F.col("policy_status") == "active", 1).otherwise(0)).alias("active_policies"),
        # added cancelled_policies - required KPI from tutorial
        F.sum(F.when(F.col("policy_status") == "cancelled", 1).otherwise(0)).alias("cancelled_policies"),
        F.round(F.sum("premium_amount"), 2).alias("premium_revenue"),
        F.round(F.avg("premium_amount"), 2).alias("avg_premium"),
        F.round(F.sum("coverage_amount"), 2).alias("total_coverage")
    )
)

gold_policy_performance.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_policy_performance"))
display(gold_policy_performance.limit(20))

