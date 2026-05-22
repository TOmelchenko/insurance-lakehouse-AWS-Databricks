# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
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
customers = spark.table(t(SILVER_SCHEMA, "silver_customers")).select(
    "customer_id", "customer_segment", "bundesland",
    "customer_age", "gdpr_consent"  # gdpr_consent carried per tutorial requirement
)
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "customer_id", "premium_amount"
)
claims = spark.table(t(SILVER_SCHEMA, "silver_claims")).select(
    "claim_id", "customer_id", "claim_amount"
)
fraud = spark.table(t(SILVER_SCHEMA, "silver_fraud_indicators")).select(
    "claim_id", "risk_score"
)

policy_agg = (
    policies
    .groupBy("customer_id")
    .agg(
        # countDistinct per tutorial - count distinct policy_id
        F.countDistinct("policy_id").alias("policy_count"),
        F.round(F.sum("premium_amount"), 2).alias("total_premium_amount")
    )
)

claims_agg = (
    claims
    .join(fraud, on="claim_id", how="left")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("claim_id").alias("claim_count"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.sum(F.when(F.col("risk_score") >= 70, 1).otherwise(0)).alias("high_risk_claims")
    )
)

# fillna only on numeric aggregation columns - avoids corrupting string columns
gold_customer_risk_profile = (
    customers
    .join(policy_agg, "customer_id", "left")
    .join(claims_agg, "customer_id", "left")
    .fillna(0, subset=["policy_count", "total_premium_amount", "claim_count",
                        "total_claim_amount", "avg_risk_score", "high_risk_claims"])
)

gold_customer_risk_profile.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_customer_risk_profile"))
display(gold_customer_risk_profile.limit(20))
