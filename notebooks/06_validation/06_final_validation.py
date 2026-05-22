# Databricks notebook source
# MAGIC %md
# MAGIC #Final Validation
# MAGIC Run final validation before submitting or presenting the project.

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "insurance_lakehouse"
gold_schema = "gold"

gold_tables = [
    "gold_claims_overview",
    "gold_policy_performance",
    "gold_customer_risk_profile",
    "gold_claims_payment_summary",
    "gold_fraud_risk_summary",
    "gold_agent_performance",
    "gold_claim_fraud_features"
]

for table in gold_tables:
    full_name = f"{catalog}.{gold_schema}.{table}"
    try:
        print(full_name, spark.table(full_name).count())
    except Exception as e:
        print(full_name, "ERROR", str(e))

# COMMAND ----------

features = spark.table(f"{catalog}.{gold_schema}.gold_claim_fraud_features")
duplicates = features.groupBy("claim_id").count().filter(F.col("count") > 1)
print("Duplicate claim_id groups:", duplicates.count())
display(duplicates.limit(20))
