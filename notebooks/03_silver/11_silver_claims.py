# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_claims"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_claims"
quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_claims"

claims_bronze = spark.table(bronze_table)

claims_prepared = (
    claims_bronze
    .withColumn("claim_id", F.trim(F.col("claim_id")))
    .withColumn("policy_id", F.trim(F.col("policy_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("claim_type", F.trim(F.col("claim_type")))
    .withColumn("claim_status", F.trim(F.col("claim_status")))
    .withColumn("reported_channel", F.trim(F.col("reported_channel")))
    .withColumn("claim_description", F.trim(F.col("claim_description")))
    .withColumn("claim_hash", F.sha2(F.col("claim_id").cast("string"), 256))
)

invalid_claims = (
    claims_prepared
    .filter(
        F.col("claim_id").isNull() |
        F.col("policy_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("claim_amount").isNull() |
        (F.col("claim_amount") < 0) |
        F.col("claim_date").isNull()
    )
    .withColumn("record_id", F.col("claim_id"))
    .withColumn("source_table", F.lit("bronze_claims"))
    .withColumn(
        "error_reason",
        F.when(F.col("claim_id").isNull(), F.lit("missing_claim_id"))
         .when(F.col("policy_id").isNull(), F.lit("missing_policy_id"))
         .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
         .when(F.col("claim_amount").isNull() | (F.col("claim_amount") < 0), F.lit("invalid_claim_amount"))
         .when(F.col("claim_date").isNull(), F.lit("missing_claim_date"))
         .otherwise(F.lit("unknown_claim_error"))
    )
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in claims_prepared.columns])))
)

valid_claims = (
    claims_prepared
    .filter(
        F.col("claim_id").isNotNull() &
        F.col("policy_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("claim_amount").isNotNull() &
        (F.col("claim_amount") >= 0) &
        F.col("claim_date").isNotNull()
    )
    .dropDuplicates(["claim_id"])
)

valid_claims.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

(
    invalid_claims
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)
)

print("Bronze claims:", claims_bronze.count())
print("Silver claims:", spark.table(silver_table).count())
print("Quarantine claims:", spark.table(quarantine_table).count())
