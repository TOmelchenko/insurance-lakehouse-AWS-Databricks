# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 Silver Validation

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

datasets = ["customers", "policies", "claims", "payments", "agents", "fraud_indicators"]

# part 1 - silver vs bronze counts
results = []
for dataset in datasets:
    bronze_table = f"{CATALOG}.bronze.bronze_{dataset}"
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_{dataset}"
    quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_{dataset}"

    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    quarantine_count = spark.table(quarantine_table).count()
    drop_count = bronze_count - silver_count - quarantine_count
    status = "PASS" if bronze_count == silver_count + quarantine_count else "REVIEW"

    results.append((dataset, bronze_count, silver_count, quarantine_count, drop_count, status))

counts_df = spark.createDataFrame(
    results,
    ["dataset", "bronze_count", "silver_count", "quarantine_count", "unaccounted_drop", "status"]
)
print("=== Silver layer counts ===")
display(counts_df)

# part 2 - quarantine error breakdown
error_results = []
for dataset in datasets:
    quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_{dataset}"
    q_df = spark.table(quarantine_table)
    if q_df.count() > 0:
        error_breakdown = (
            q_df
            .groupBy("error_reason", "error_severity")
            .count()
            .withColumn("dataset", F.lit(dataset))
            .select("dataset", "error_reason", "error_severity", "count")
        )
        error_results.append(error_breakdown)

if error_results:
    from functools import reduce
    error_df = reduce(lambda a, b: a.union(b), error_results)
    print("=== Quarantine error breakdown ===")
    display(error_df)
else:
    print("No quarantine records found across all datasets")

# part 3 - silver metadata check
metadata_results = []
for dataset in datasets:
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_{dataset}"
    columns = spark.table(silver_table).columns
    metadata_results.append((
        dataset,
        "ingest_timestamp" in columns,
        "ingest_run_id" in columns,
        "source_file_name" in columns
    ))

metadata_df = spark.createDataFrame(
    metadata_results,
    ["dataset", "has_ingest_timestamp", "has_ingest_run_id", "has_source_file_name"]
)
print("=== Silver metadata check ===")
display(metadata_df)

# part 4 - save validation summary
counts_df.withColumn("validation_timestamp", F.current_timestamp()) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.day2_silver_validation_summary")

print("Validation summary saved to day2_silver_validation_summary")
