# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_payments"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_payments"
quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_payments"

payments_bronze = spark.table(bronze_table)

payments_prepared = (
    payments_bronze
    .withColumn("payment_id", F.trim(F.col("payment_id")))
    .withColumn("claim_id", F.trim(F.col("claim_id")))
    .withColumn("payment_status", F.trim(F.col("payment_status")))
    .withColumn("payment_method", F.trim(F.col("payment_method")))
    .withColumn("payment_hash", F.sha2(F.col("payment_id").cast("string"), 256))
)

invalid_payments = (
    payments_prepared
    .filter(
        F.col("payment_id").isNull() |
        F.col("claim_id").isNull() |
        F.col("payment_amount").isNull() |
        (F.col("payment_amount") < 0) |
        F.col("payment_date").isNull()
    )
    .withColumn("record_id", F.col("payment_id"))
    .withColumn("source_table", F.lit("bronze_payments"))
    .withColumn(
        "error_reason",
        F.when(F.col("payment_id").isNull(), F.lit("missing_payment_id"))
         .when(F.col("claim_id").isNull(), F.lit("missing_claim_id"))
         .when(F.col("payment_amount").isNull() | (F.col("payment_amount") < 0), F.lit("invalid_payment_amount"))
         .when(F.col("payment_date").isNull(), F.lit("missing_payment_date"))
         .otherwise(F.lit("unknown_payment_error"))
    )
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in payments_prepared.columns])))
)

valid_payments = (
    payments_prepared
    .filter(
        F.col("payment_id").isNotNull() &
        F.col("claim_id").isNotNull() &
        F.col("payment_amount").isNotNull() &
        (F.col("payment_amount") >= 0) &
        F.col("payment_date").isNotNull()
    )
    .dropDuplicates(["payment_id"])
)

valid_payments.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

(
    invalid_payments
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)
)

print("Bronze payments:", payments_bronze.count())
print("Silver payments:", spark.table(silver_table).count())
print("Quarantine payments:", spark.table(quarantine_table).count())
