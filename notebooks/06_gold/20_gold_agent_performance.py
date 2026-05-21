# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — Gold Agent Performance
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
agents = spark.table(t(SILVER_SCHEMA, "silver_agents")).select(
    "agent_id", "agent_name", "region", "bundesland", "commission_rate", "active_flag"
)
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "agent_id", "premium_amount", "policy_status"
)
claims = spark.table(t(SILVER_SCHEMA, "silver_claims")).select(
    "claim_id", "policy_id", "claim_amount"
)
payments = spark.table(t(SILVER_SCHEMA, "silver_payments")).select(
    "claim_id", "payment_amount"
)

policies_by_agent = (
    policies
    .groupBy("agent_id")
    .agg(
        # countDistinct per tutorial requirement
        F.countDistinct("policy_id").alias("total_policies_sold"),
        # added active_policies per tutorial KPI list
        F.sum(F.when(F.col("policy_status") == "active", 1).otherwise(0)).alias("active_policies"),
        F.round(F.sum("premium_amount"), 2).alias("premium_revenue")
    )
)

claims_by_agent = (
    claims
    .join(policies.select("policy_id", "agent_id"), on="policy_id", how="left")
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("total_claims_linked"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount")
    )
)

payments_by_agent = (
    payments
    .join(claims.select("claim_id", "policy_id"), on="claim_id", how="left")
    .join(policies.select("policy_id", "agent_id"), on="policy_id", how="left")
    .groupBy("agent_id")
    .agg(F.round(F.sum("payment_amount"), 2).alias("total_paid_amount"))
)

# fillna only on numeric aggregation columns - avoids corrupting string columns
gold_agent_performance = (
    agents
    .join(policies_by_agent, on="agent_id", how="left")
    .join(claims_by_agent, on="agent_id", how="left")
    .join(payments_by_agent, on="agent_id", how="left")
    .fillna(0, subset=["total_policies_sold", "active_policies", "premium_revenue",
                        "total_claims_linked", "total_claim_amount", "total_paid_amount"])
    .withColumn(
        "claims_ratio",
        F.when(
            F.col("premium_revenue").isNull() | (F.col("premium_revenue") == 0), None
        ).otherwise(F.round(F.col("total_claim_amount") / F.col("premium_revenue"), 4))
    )
    .withColumn("estimated_commission", F.round(F.col("premium_revenue") * F.col("commission_rate"), 2))
)

gold_agent_performance.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_agent_performance"))
display(gold_agent_performance.limit(20))

