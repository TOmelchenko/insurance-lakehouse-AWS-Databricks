# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_policies"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_policies"
quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_policies"

# valid values from quality_rules.yml
VALID_POLICY_STATUS = ["active", "cancelled", "expired"]
VALID_POLICY_TYPE = ["car", "home", "health", "travel", "liability"]

policies_bronze = spark.table(bronze_table)

policies_prepared = (
    policies_bronze
    .withColumn("policy_id", F.trim(F.col("policy_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("agent_id", F.trim(F.col("agent_id")))
    .withColumn("policy_type", F.lower(F.trim(F.col("policy_type"))))
    .withColumn("policy_status", F.lower(F.trim(F.col("policy_status"))))
    .withColumn("sales_channel", F.trim(F.col("sales_channel")))
    .withColumn("policy_duration_days", F.datediff(F.col("end_date"), F.col("start_date")))
    .withColumn("policy_hash", F.sha2(F.col("policy_id").cast("string"), 256))
)

invalid_policies = (
    policies_prepared
    .filter(
        F.col("policy_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("premium_amount").isNull() |
        (F.col("premium_amount") < 0) |
        F.col("coverage_amount").isNull() |
        (F.col("coverage_amount") < 0) |
        F.col("start_date").isNull() |
        F.col("end_date").isNull() |
        (F.col("end_date") < F.col("start_date")) |
        ~F.col("policy_status").isin(VALID_POLICY_STATUS) |
        ~F.col("policy_type").isin(VALID_POLICY_TYPE)
    )
    .withColumn("record_id", F.col("policy_id"))
    .withColumn("source_table", F.lit("bronze_policies"))
    .withColumn(
        "error_reason",
        F.when(F.col("policy_id").isNull(), F.lit("missing_policy_id"))
         .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
         .when(F.col("premium_amount").isNull() | (F.col("premium_amount") < 0), F.lit("invalid_premium_amount"))
         .when(F.col("coverage_amount").isNull() | (F.col("coverage_amount") < 0), F.lit("invalid_coverage_amount"))
         .when(F.col("start_date").isNull() | F.col("end_date").isNull(), F.lit("missing_dates"))
         .when(F.col("end_date") < F.col("start_date"), F.lit("end_date_before_start_date"))
         .when(~F.col("policy_status").isin(VALID_POLICY_STATUS), F.lit("invalid_policy_status"))
         .when(~F.col("policy_type").isin(VALID_POLICY_TYPE), F.lit("invalid_policy_type"))
         .otherwise(F.lit("unknown_policy_error"))
    )
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in policies_prepared.columns])))
)

valid_policies = (
    policies_prepared
    .filter(
        F.col("policy_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("premium_amount").isNotNull() &
        (F.col("premium_amount") >= 0) &
        F.col("coverage_amount").isNotNull() &
        (F.col("coverage_amount") >= 0) &
        F.col("start_date").isNotNull() &
        F.col("end_date").isNotNull() &
        (F.col("end_date") >= F.col("start_date")) &
        F.col("policy_status").isin(VALID_POLICY_STATUS) &
        F.col("policy_type").isin(VALID_POLICY_TYPE)
    )
    .dropDuplicates(["policy_id"])
)

valid_policies.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

(
    invalid_policies
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)
)

print("Bronze policies:", policies_bronze.count())
print("Silver policies:", spark.table(silver_table).count())
print("Quarantine policies:", spark.table(quarantine_table).count())
