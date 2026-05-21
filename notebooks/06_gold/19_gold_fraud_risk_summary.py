# Databricks notebook source
# MAGIC %md
# MAGIC # 19 — Gold Fraud Risk Summary
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
claims = spark.table(t(SILVER_SCHEMA, "silver_claims")).select(
    "claim_id", "policy_id", "customer_id", "claim_type", "claim_amount"
)
fraud = spark.table(t(SILVER_SCHEMA, "silver_fraud_indicators")).select(
    "claim_id", "risk_score", "suspicious_amount_flag",
    "duplicate_claim_flag", "late_report_flag"
)
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "policy_type"
)
customers = spark.table(t(SILVER_SCHEMA, "silver_customers")).select(
    "customer_id", "bundesland"
)

fraud_enriched = (
    claims
    .join(fraud, on="claim_id", how="left")
    .join(policies, on="policy_id", how="left")
    .join(customers, on="customer_id", how="left")
    # fixed risk band thresholds to match tutorial: high >= 70, medium 30-69, low < 30
    .withColumn(
        "risk_band",
        F.when(F.col("risk_score") >= 70, "high")
         .when(F.col("risk_score") >= 30, "medium")
         .otherwise("low")
    )
)

gold_fraud_risk_summary = (
    fraud_enriched
    .groupBy("bundesland", "policy_type", "claim_type", "risk_band")
    .agg(
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("risk_band") == "high", 1).otherwise(0)).alias("high_risk_claims"),
        # added high_risk_rate per kpi_definitions.yml: high_risk_claims / total_claims
        F.round(
            F.sum(F.when(F.col("risk_band") == "high", 1).otherwise(0)) / F.count("*"), 4
        ).alias("fraud_risk_rate"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.sum(F.when(F.col("suspicious_amount_flag") == True, 1).otherwise(0)).alias("suspicious_amount_count"),
        F.sum(F.when(F.col("duplicate_claim_flag") == True, 1).otherwise(0)).alias("duplicate_claim_count"),
        F.sum(F.when(F.col("late_report_flag") == True, 1).otherwise(0)).alias("late_report_count")
    )
)

gold_fraud_risk_summary.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_fraud_risk_summary"))
display(gold_fraud_risk_summary.limit(20))

