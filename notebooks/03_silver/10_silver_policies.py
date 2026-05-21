# Databricks notebook source
# Week 11 Day 2 — Silver Policies starter template

from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

valid_policy_status = ["active", "cancelled", "expired"]
valid_policy_type = ["car", "home", "health", "travel", "liability"]

policies_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_policies")
customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_customers").select("customer_id").dropDuplicates()

policies_prepared = (
    policies_bronze
    .withColumn("policy_id", F.trim(F.col("policy_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("policy_type", F.lower(F.trim(F.col("policy_type"))))
    .withColumn("policy_status", F.lower(F.trim(F.col("policy_status"))))
    .withColumn("premium_amount", F.col("premium_amount").cast("double"))
    .withColumn("coverage_amount", F.col("coverage_amount").cast("double"))
    .withColumn("start_date", F.to_date(F.col("start_date")))
    .withColumn("end_date", F.to_date(F.col("end_date")))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
)

policies_joined = (
    policies_prepared.alias("p")
    .join(customers.alias("c"), F.col("p.customer_id") == F.col("c.customer_id"), "left")
    .withColumn("customer_exists", F.col("c.customer_id").isNotNull())
    .select("p.*", "customer_exists")
)

invalid_policies = (
    policies_joined
    .filter(
        F.col("policy_id").isNull()
        | F.col("customer_id").isNull()
        | (~F.col("customer_exists"))
        | (~F.col("policy_type").isin(valid_policy_type))
        | (~F.col("policy_status").isin(valid_policy_status))
        | (F.col("premium_amount") <= 0)
        | (F.col("coverage_amount") <= F.col("premium_amount"))
    )
    .withColumn("record_id", F.col("policy_id"))
    .withColumn("source_table", F.lit("bronze_policies"))
    .withColumn("error_reason", F.lit("policy_quality_rule_failed"))
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in policies_prepared.columns])))
)

valid_policies = (
    policies_joined
    .filter(
        F.col("policy_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("customer_exists")
        & F.col("policy_type").isin(valid_policy_type)
        & F.col("policy_status").isin(valid_policy_status)
        & (F.col("premium_amount") > 0)
        & (F.col("coverage_amount") > F.col("premium_amount"))
    )
    .drop("customer_exists")
    .dropDuplicates(["policy_id"])
)

valid_policies.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_policies")

(
    invalid_policies
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_policies")
)

print("Bronze policies:", policies_bronze.count())
print("Silver policies:", spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_policies").count())
print("Quarantine policies:", spark.table(f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_policies").count())
