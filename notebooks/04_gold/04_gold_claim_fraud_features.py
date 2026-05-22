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
    "claim_id", "policy_id", "customer_id", "claim_date",
    "claim_type", "claim_status", "claim_amount", "fraud_flag"
)
policies = spark.table(t(SILVER_SCHEMA, "silver_policies")).select(
    "policy_id", "policy_type", "start_date", "premium_amount", "coverage_amount"
)
# select date_of_birth only for deriving customer_age - will be dropped after
customers = spark.table(t(SILVER_SCHEMA, "silver_customers")).select(
    "customer_id", "date_of_birth", "bundesland"
)
payments = spark.table(t(SILVER_SCHEMA, "silver_payments")).select(
    "claim_id", "payment_date", "payment_amount"
)
# select all required fraud feature columns per tutorial page 19
fraud = spark.table(t(SILVER_SCHEMA, "silver_fraud_indicators")).select(
    "claim_id", "risk_score", "risk_category",
    "previous_claims_count",       # added - required feature per tutorial
    "suspicious_amount_flag",
    "duplicate_claim_flag",
    "late_report_flag",
    "high_risk_region_flag"        # added - required feature per tutorial
)

# aggregate payments to one row per claim before joining
payments_by_claim = (
    payments
    .groupBy("claim_id")
    .agg(
        F.min("payment_date").alias("first_payment_date"),
        F.round(F.sum("payment_amount"), 2).alias("total_paid_amount")
    )
)

gold_claim_fraud_features = (
    claims
    .join(policies, on="policy_id", how="left")
    .join(customers, on="customer_id", how="left")
    .join(payments_by_claim, on="claim_id", how="left")
    .join(fraud, on="claim_id", how="left")
    # handle both null and zero coverage_amount
    .withColumn(
        "claim_amount_to_coverage_ratio",
        F.when(
            F.col("coverage_amount").isNull() | (F.col("coverage_amount") == 0), None
        ).otherwise(F.round(F.col("claim_amount") / F.col("coverage_amount"), 4))
    )
    .withColumn("policy_age_days", F.datediff(F.col("claim_date"), F.col("start_date")))
    # derive customer_age from date_of_birth then drop raw date field - PII rule
    .withColumn("customer_age", F.floor(F.datediff(F.col("claim_date"), F.col("date_of_birth")) / F.lit(365.25)))
    .drop("date_of_birth")
    .withColumn("payment_delay_days", F.datediff(F.col("first_payment_date"), F.col("claim_date")))
)

# grain validation - confirm one row per claim_id
duplicate_count = (
    gold_claim_fraud_features
    .groupBy("claim_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"Duplicate claim_id rows: {duplicate_count}")
assert duplicate_count == 0, "Grain violation: duplicate claim_id found in gold_claim_fraud_features"

gold_claim_fraud_features.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(t(GOLD_SCHEMA, "gold_claim_fraud_features"))
display(gold_claim_fraud_features.limit(20))
