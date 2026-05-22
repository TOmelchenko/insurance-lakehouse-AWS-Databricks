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
    "claim_id", "policy_id", "claim_date", "claim_status",
    "claim_type", "claim_amount"
)
payments = spark.table(t(SILVER_SCHEMA, "silver_payments")).select(
    "claim_id", "payment_amount", "payment_status", "payment_date"
)
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "policy_type", "premium_amount", "coverage_amount"
)

# aggregate payments to one row per claim before joining - avoids grain duplication
payments_by_claim = (
    payments
    .groupBy("claim_id")
    .agg(
        F.count("*").alias("payment_count"),
        F.round(F.sum("payment_amount"), 2).alias("total_paid_amount"),
        # renamed to match tutorial pattern: payment_rejection_count
        F.sum(F.when(F.col("payment_status") == "rejected", 1).otherwise(0)).alias("payment_rejection_count"),
        F.min("payment_date").alias("first_payment_date"),
        # added last_payment_date per tutorial payment summary pattern
        F.max("payment_date").alias("last_payment_date")
    )
)

gold_claims_payment_summary = (
    claims
    .join(policies, on="policy_id", how="left")
    .join(payments_by_claim, on="claim_id", how="left")
    .withColumn("payment_delay_days", F.datediff(F.col("first_payment_date"), F.col("claim_date")))
    # handle both null and zero claim_amount to avoid divide-by-zero
    .withColumn(
        "claim_to_payment_ratio",
        F.when(
            F.col("claim_amount").isNull() | (F.col("claim_amount") == 0), None
        ).otherwise(F.round(F.col("total_paid_amount") / F.col("claim_amount"), 4))
    )
)

gold_claims_payment_summary.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_claims_payment_summary"))
display(gold_claims_payment_summary.limit(20))
