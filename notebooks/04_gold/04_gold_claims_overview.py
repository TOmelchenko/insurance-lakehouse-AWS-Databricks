# Databricks notebook source
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
claims = spark.table(t(SILVER_SCHEMA, "silver_claims")).select(
    "claim_id", "policy_id", "customer_id",
    "claim_date", "claim_type", "claim_status",
    "claim_amount", "fraud_flag"
)
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "policy_type"
)
customers = spark.table(t(SILVER_SCHEMA, "silver_customers")).select(
    "customer_id", "bundesland"
)
fraud = spark.table(t(SILVER_SCHEMA, "silver_fraud_indicators")).select(
    "claim_id", "risk_score"
)

claims_enriched = (
    claims
    .join(policies, on="policy_id", how="left")
    .join(customers, on="customer_id", how="left")
    .join(fraud, on="claim_id", how="left")
    .withColumn("claim_month", F.date_trunc("month", F.col("claim_date")).cast("date"))
)

gold_claims_overview = (
    claims_enriched
    .groupBy("claim_month", "claim_status", "claim_type", "policy_type", "bundesland")
    .agg(
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("claim_status") == "open", 1).otherwise(0)).alias("open_claims"),
        F.sum(F.when(F.col("claim_status") == "approved", 1).otherwise(0)).alias("approved_claims"),
        F.sum(F.when(F.col("claim_status") == "rejected", 1).otherwise(0)).alias("rejected_claims"),
        F.sum(F.when(F.col("claim_status") == "paid", 1).otherwise(0)).alias("paid_claims"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount"),
        # renamed to match tutorial KPI naming convention
        F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        # added fraud_flag_rate - required KPI from tutorial
        F.round(F.avg(F.col("fraud_flag").cast("int")), 4).alias("fraud_flag_rate")
    )
)

gold_claims_overview.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_claims_overview"))
display(gold_claims_overview.limit(20))
