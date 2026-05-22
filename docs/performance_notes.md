# Performance Notes
## Insurance Lakehouse - Rheinland Versicherung AG

---

## Compute environment

- **Compute:** Databricks Serverless (small mode dataset)
- **Dataset size:** 186,000 total rows across 6 datasets
- **Runtime observations:** all silver notebooks completed in under 30 seconds; gold notebooks in under 15 seconds

---

## Autoloader ingestion

Autoloader (`cloudFiles`) was chosen over `spark.read.csv` for bronze ingestion because it processes files incrementally. Once a file is recorded in the checkpoint, it is never reprocessed on subsequent runs. This is efficient for real-world scenarios where new files land in S3 daily.

**Schema hints** were required for all boolean fields. Without them, Autoloader infers boolean CSV values (`true`/`false`) as strings, causing a `DELTA_MERGE_INCOMPATIBLE_DATATYPE` error when writing to an existing Delta table with a boolean schema. The affected fields were:

- `customers.gdpr_consent`
- `claims.fraud_flag`
- `agents.active_flag`
- `fraud_indicators.suspicious_amount_flag`, `duplicate_claim_flag`, `late_report_flag`, `high_risk_region_flag`

**Checkpoint location:** `s3://insurance-lakehouse-project/checkpoints/{dataset}/` - stored in S3 so checkpoints survive cluster restarts and workspace changes.

---

## Column pruning

All gold notebooks select only required columns before joins. For example:

```python
claims = spark.table("silver_claims").select(
    "claim_id", "policy_id", "customer_id",
    "claim_date", "claim_type", "claim_status", "claim_amount", "fraud_flag"
)
```

This reduces the data carried through the execution plan, especially important for wide tables like `silver_claims` which has 14 columns.

---

## Aggregate before join

The most important performance pattern applied in this project is aggregating one-to-many tables before joining. For example, payments has multiple rows per claim. Joining raw payments directly to claims would multiply claim rows.

```python
# aggregate first - one row per claim
payments_by_claim = payments.groupBy("claim_id").agg(
    F.sum("payment_amount").alias("total_paid_amount"),
    F.min("payment_date").alias("first_payment_date"),
    F.max("payment_date").alias("last_payment_date")
)

# then join to claims - grain is preserved
gold_claims_payment_summary = claims.join(payments_by_claim, "claim_id", "left")
```

This pattern is applied in `gold_claims_payment_summary`, `gold_agent_performance`, and `gold_claim_fraud_features`.

---

## Window function for deduplication

`silver_fraud_indicators` uses a window function to keep one record per `claim_id` rather than `dropDuplicates`. This ensures the record with the highest `risk_score` is kept, which is more meaningful than an arbitrary row selection.

```python
window = Window.partitionBy("claim_id").orderBy(F.col("risk_score").desc())
valid_fraud = fraud_prepared.withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1).drop("row_num")
```

This reduced 50,000 bronze rows to 31,694 silver rows - the correct number of distinct claims.

---

## Anti-join for foreign key validation

Foreign key validation uses Spark `left_anti` joins rather than filtering after a left join. Anti-join is more efficient because it stops as soon as it finds a non-match and does not carry unneeded columns.

```python
invalid_fk = field_valid.join(
    silver_customers.select("customer_id"),
    on="customer_id",
    how="left_anti"
)
```

---

## Delta write options

All silver and gold tables use:
```python
.option("overwriteSchema", "true")
```

This allows schema changes during development without manually dropping tables. In production this would be replaced with schema migration controls.

---

## Explain plan - gold_claim_fraud_features

The following observations come from running `gold_claim_fraud_features.explain(True)` in the notebook.

**Parsed plan** - single `UnresolvedRelation` pointing to the gold table. This is a simple read plan because the table was already written - Spark reads from the materialized Delta file rather than re-executing the joins.

**Physical plan** - `PhotonScan parquet` with no `DataFilters`, `PartitionFilters`, or `RequiredDataFilters`. This means the full table is scanned. For dashboard queries that filter by `bundesland` or `claim_type`, partitioning by those columns would allow Spark to skip irrelevant files.

**Photon** - the query is fully supported by Photon (Databricks vectorized execution engine). No fallback to standard Spark execution.

**Storage** - table stored as Parquet in Unity Catalog managed storage on S3 (`s3://dbstorage-prod-acakk/uc/...`). Single file path, no partitioning.

**Statistics** - `full` statistics available for `gold_claim_fraud_features` (50,000 rows). Full statistics allow the Spark optimizer to make better join and aggregation decisions when this table is used as an input.

---

## OPTIMIZE

`OPTIMIZE` was run on `gold_claim_fraud_features` and completed successfully. For a 50,000-row table the impact is minimal, but it is good practice - OPTIMIZE compacts small Delta files into larger ones, reducing the number of S3 read requests on future scans.

For larger datasets (medium/large mode), OPTIMIZE combined with `ZORDER BY (claim_id, policy_id)` would co-locate related rows and significantly speed up join-heavy queries:

```sql
OPTIMIZE insurance_lakehouse.gold.gold_claim_fraud_features
ZORDER BY (claim_id, policy_id);
```

---



The current dataset runs in small mode (186,000 rows). The data generation script supports medium (7M+ rows) and large (47M+ rows) modes. For larger datasets:

- Increase `PARTITIONS` from 8 to 128 or 512 in the generation script
- Switch from Serverless to a classic cluster with multiple workers for silver transformations
- Consider partitioning gold tables by `claim_month` for time-filtered dashboard queries
- Use `OPTIMIZE` and `ZORDER` on high-cardinality join keys like `claim_id` and `policy_id`
