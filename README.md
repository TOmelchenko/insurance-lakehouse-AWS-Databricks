# Insurance Lakehouse - Rheinland Versicherung AG

End-to-end data engineering pipeline for a synthetic German insurance company, built on Databricks Unity Catalog with AWS S3, following a bronze-silver-gold medallion architecture.

---

## Business context

**Rheinland Versicherung AG** is a fictional German insurance company operating across 9 Bundesländer. The business faced three core data problems:

- **Siloed data** - customer, policy, claims, payment, and agent data lived in separate systems with no unified view, making cross-domain analytics impossible
- **No data quality gate** - raw data flowed directly into reporting with no validation, so invalid records, missing keys, and impossible dates reached analysts silently
- **Fraud blind spot** - no systematic fraud risk scoring or regional fraud pattern monitoring across the portfolio

This project builds a lakehouse that solves all three: unified data, enforced quality, and fraud analytics.

---

## Technology stack

| Layer | Technology |
|---|---|
| Compute | Databricks Serverless + Unity Catalog |
| Storage | AWS S3 (external location via CloudFormation) |
| Ingestion | Databricks Autoloader (`cloudFiles`) |
| Table format | Delta Lake |
| Language | PySpark (Python 3) |
| Configuration | YAML (`quality_rules.yml`, `pii_config.yml`) |
| Infrastructure | AWS IAM, AWS CloudFormation, Databricks Unity Catalog |

---

## Architecture overview

```
Synthetic data generation (Databricks Serverless)
        ↓
S3 raw CSV files  (s3://insurance-lakehouse-project/raw/)
        ↓
Autoloader (cloudFiles) - incremental ingestion with S3 checkpoints
        ↓
Bronze layer      - raw arrival, ingestion metadata, no transformations
        ↓
Silver layer      - cleaned, validated, PII-handled, quarantine routing
        ↓
Gold layer        - KPIs, fraud analytics, AI-ready features
        ↓
Dashboard layer   - 6 SQL views, Databricks Lakeview charts
```

---

## Data sources

Six synthetic datasets modelling a German insurance company:

| Dataset | Rows (small) | Description |
|---|---|---|
| customers | 10,000 | Demographics, GDPR consent, customer segment |
| policies | 25,000 | Insurance products, premium, coverage, status |
| claims | 50,000 | Claim events, amounts, types, fraud flag |
| payments | 50,000 | Settlement payments per claim |
| agents | 1,000 | Broker profiles, commission rates, region |
| fraud_indicators | 50,000 | Risk scores and fraud flags per claim |

---

## Data size modes

The data generator supports four modes controlled by `DATA_MODE`:

| Mode | Customers | Claims | Payments | Fraud indicators | Partitions |
|---|---|---|---|---|---|
| `test` | 10 | 500 | 500 | 500 | 8 |
| `small` | 10,000 | 50,000 | 50,000 | 50,000 | 8 |
| `medium` | 500,000 | 5,000,000 | 5,000,000 | 5,000,000 | 128 |
| `large` | 2,000,000 | 20,000,000 | 20,000,000 | 20,000,000 | 512 |

All validation, silver, and gold logic is mode-agnostic - only the row counts change.

---

## S3 folder structure

```
s3://insurance-lakehouse-project/
  raw/
    customers/          - source CSV files for customers
    policies/           - source CSV files for policies
    claims/             - source CSV files for claims
    payments/           - source CSV files for payments
    agents/             - source CSV files for agents
    fraud_indicators/   - source CSV files for fraud indicators
  checkpoints/
    customers/          - Autoloader checkpoint (tracks processed files)
    policies/
    claims/
    payments/
    agents/
    fraud_indicators/
```

Checkpoints are stored in S3 so they survive cluster restarts and workspace changes. Once a file is recorded in the checkpoint, Autoloader never reprocesses it.

---

## Bronze layer

Synthetic CSV data is written to S3 then loaded incrementally into Delta tables using **Databricks Autoloader** (`cloudFiles`).

**Key design decisions:**

- `trigger(availableNow=True)` - runs as a one-shot batch, not a continuous stream
- `_metadata.file_path` used instead of `input_file_name()` - required for Unity Catalog
- Schema hints required for all boolean fields (CSV reads them as strings)

**Schema hints by dataset:**

| Dataset | Boolean fields requiring hints |
|---|---|
| customers | `gdpr_consent` |
| claims | `fraud_flag` |
| agents | `active_flag` |
| fraud_indicators | `suspicious_amount_flag`, `duplicate_claim_flag`, `late_report_flag`, `high_risk_region_flag` |

**Run modes:**

- `FULL_RELOAD = True` - truncates bronze tables and clears checkpoints. Use when regenerating all synthetic data from scratch.
- `FULL_RELOAD = False` - incremental mode. Autoloader picks up only new files. Validation compares rows added this run rather than total counts.

**Audit columns added at ingestion:**

| Column | Description |
|---|---|
| `ingest_timestamp` | When the record was loaded |
| `ingest_run_id` | UUID linking all records from the same pipeline run |
| `source_file_name` | S3 path of the source file |

---

## Silver layer

Each silver notebook reads from bronze, applies cleaning and validation, routes invalid records to quarantine, and writes trusted Delta tables.

**Cleaning steps:**
- Text fields trimmed and normalised (`initcap` for names, `lower` for statuses)
- Dates cast to correct types
- Boolean fields cast explicitly
- Derived fields added: `customer_age`, `policy_duration_days`, `risk_category`, `fraud_indicator_count`

**Validation rules:**
- Primary key null checks
- Foreign key validation using `left_anti` joins across policies, claims, and payments
- Valid value enforcement per `quality_rules.yml`
- Business logic checks: `coverage_amount > premium_amount`, `payment_date >= claim_date`, `claim_amount > 0`

**PII handling per `pii_config.yml`:**
- Dropped at silver: `first_name`, `last_name`, `email`, `phone_number`, `street`, `postal_code`
- Hashed SHA-256: `email_hash`, `phone_hash`, `customer_hash`
- GDPR consent enforced - records with null `gdpr_consent` quarantined and excluded from all downstream tables

**Deduplication:**
- Standard datasets: `dropDuplicates` on primary key
- `fraud_indicators`: window function `row_number()` over `risk_score DESC` per `claim_id` - keeps highest-risk record, not an arbitrary one

**Silver validation results (small mode):**

| Dataset | Bronze | Silver | Quarantine | Status |
|---|---|---|---|---|
| customers | 10,000 | 10,000 | 0 | PASS |
| policies | 25,000 | 25,000 | 0 | PASS |
| claims | 50,000 | 50,000 | 0 | PASS |
| payments | 50,000 | 27,494 | 22,506 | PASS |
| agents | 1,000 | 1,000 | 0 | PASS |
| fraud_indicators | 50,000 | 31,694 | 0* | REVIEW |

*18,306 deduplicated by design - highest risk_score kept per claim_id.

---

## Gold layer

7 Gold tables built for business reporting and AI feature engineering. All one-row-per-entity tables validated for zero duplicate grain.

| Table | Purpose | Grain | Rows |
|---|---|---|---|
| gold_claims_overview | Claims operations reporting | month + status + type + product + region | 30,189 |
| gold_policy_performance | Portfolio and premium analytics | product + status + channel + region | 540 |
| gold_customer_risk_profile | Customer-level risk summary | one row per customer | 10,000 |
| gold_claims_payment_summary | Claim settlement reporting | one row per claim | 50,000 |
| gold_fraud_risk_summary | Fraud monitoring by region and product | region + product + claim type + risk band | 810 |
| gold_agent_performance | Broker/agent KPIs | one row per agent | 1,000 |
| gold_claim_fraud_features | AI-ready fraud feature table | one row per claim | 50,000 |

**Core KPIs:**

| KPI | Formula |
|---|---|
| `total_claims` | `count(*)` |
| `premium_revenue` | `sum(premium_amount)` |
| `claims_ratio` | `total_claim_amount / premium_revenue` |
| `fraud_risk_rate` | `high_risk_claims / total_claims` |

**Risk bands** used in fraud tables: `low` (score < 30), `medium` (30-69), `high` (>= 70).

---

## Dashboard layer

6 SQL views created on gold tables for Databricks Lakeview dashboards:

| View | Source | Chart type |
|---|---|---|
| `vw_executive_insurance_overview` | gold_policy_performance | KPI tiles |
| `vw_claims_operations` | gold_claims_overview | Line chart, bar chart, donut chart |
| `vw_policy_portfolio` | gold_policy_performance | Bar chart, donut chart |
| `vw_fraud_risk_monitoring` | gold_fraud_risk_summary | Bar chart |
| `vw_agent_regional_performance` | gold_agent_performance | Bar chart |
| `vw_data_quality_monitoring` | all quarantine tables | Bar chart |

---

## Data quality and quarantine

Invalid records are never silently dropped. They are routed to dedicated quarantine tables with full error context.

**Quarantine tables:** `quarantine_invalid_customers`, `quarantine_invalid_policies`, `quarantine_invalid_claims`, `quarantine_invalid_payments`

**Quarantine schema:**

| Column | Purpose |
|---|---|
| `record_id` | Primary key of the rejected record |
| `source_table` | Which bronze table it came from |
| `error_reason` | Typed validation failure (e.g. `payment_date_before_claim_date`) |
| `error_severity` | HIGH / MEDIUM / LOW |
| `quarantine_timestamp` | When it was quarantined |
| `original_record_json` | Full original record for audit and debugging |

**Valid value rules (quality_rules.yml):**

| Field | Valid values |
|---|---|
| `policy_status` | active, cancelled, expired |
| `policy_type` | car, home, health, travel, liability |
| `claim_status` | open, approved, rejected, under_review, paid |
| `payment_status` | paid, pending, rejected |
| `payment_method` | SEPA, bank_transfer, card |
| `risk_score` | 0-100 |

---

## GDPR and governance

**PII fields (pii_config.yml):**

| Field | Treatment |
|---|---|
| `first_name`, `last_name` | Dropped at silver |
| `email`, `phone_number` | Hashed SHA-256, original dropped |
| `street`, `postal_code` | Dropped at silver |
| `date_of_birth` | Retained, used to derive `customer_age` only |
| `iban_hash` | Pre-hashed at source |
| `gdpr_consent` | Enforced - null = quarantine |

**Role-based access design:**

| Layer | Access |
|---|---|
| Bronze | Data engineers only - contains raw PII |
| Silver | Data analysts, data scientists - PII masked |
| Gold | Business users, BI tools - aggregated only |
| Quarantine | Data engineers, compliance team |

**GDPR article mapping:**

| Article | Implementation |
|---|---|
| Art. 5 - Data minimisation | PII dropped or hashed at silver |
| Art. 6 - Lawful basis | `gdpr_consent` enforced before analytics |
| Art. 25 - Privacy by design | Masking applied at pipeline level |
| Art. 30 - Records of processing | Audit columns on every table |
| Art. 32 - Security | Unity Catalog role-based access |

---

## Performance considerations

- **Column pruning** - only required columns selected before joins, reducing data carried through the execution plan
- **Aggregate before join** - payments aggregated to one row per claim before joining to claims, preventing grain duplication
- **Anti-join for FK validation** - `left_anti` join stops as soon as a non-match is found, more efficient than post-join filtering
- **Window over dropDuplicates** - `row_number()` over `risk_score DESC` keeps the most meaningful record rather than an arbitrary one
- **Photon** - `explain(True)` on `gold_claim_fraud_features` confirmed full Photon support, no fallback to standard Spark
- **OPTIMIZE** - run on `gold_claim_fraud_features` to compact Delta files. For medium/large mode, add `ZORDER BY (claim_id, policy_id)`
- **fillna scoped to numeric columns** - prevents string columns like `bundesland` being overwritten with `0`

---

## How to run the project

### Prerequisites

- Databricks workspace on AWS with Unity Catalog enabled
- S3 bucket in the same region as your workspace
- External location set up via Databricks Quickstart + AWS CloudFormation (see below)

### AWS - Databricks connection setup

Serverless compute authenticates through Unity Catalog - direct credentials (`spark.conf.set`) do not work.

1. In your workspace: **Catalog > gear icon > External Locations > Create external location > Quickstart**
2. Enter your S3 bucket name and copy the pre-generated Personal Access Token
3. Proceed to AWS Console - a CloudFormation form opens pre-filled
4. Paste the token and click **Create stack** (takes 1-2 minutes)
5. The stack creates the IAM role, instance profile, bucket policy, and registers the external location in Unity Catalog
6. Back in Databricks: **External Locations > Test connection** - all checks should pass

> Make sure you are in the correct AWS region (matching your workspace and bucket) when creating the CloudFormation stack.

### Run order

```
00_setup/00_project_setup                    - create catalog and schemas
01_data_generation/01_generate_...s3        - generate synthetic data, write to S3
02_bronze/02_bronze_ingestion_s3_autoloader  - ingest from S3 to bronze Delta tables
03_silver/03_silver_customers                - silver layer (run all 6)
...
03_silver/03_silver_fraud_indicators
04_gold/04_gold_claims_overview              - gold layer (run all 7)
...
04_gold/04_gold_claim_fraud_features
05_dashboards/05_dashboard_views             - create SQL views
06_validation/06_final_validation            - validate all layers
```

### Full reload vs incremental

```python
# In 02_bronze_ingestion_s3_autoloader:
FULL_RELOAD = True   # truncates tables + clears checkpoints - use for fresh data
FULL_RELOAD = False  # incremental - only new S3 files processed
```

---

## Final outputs

| Output | Location |
|---|---|
| 6 bronze Delta tables | `insurance_lakehouse.bronze.*` |
| 6 silver Delta tables | `insurance_lakehouse.silver.*` |
| 4 quarantine tables | `insurance_lakehouse.quarantine.*` |
| 7 gold Delta tables | `insurance_lakehouse.gold.*` |
| 6 dashboard SQL views | `insurance_lakehouse.gold.vw_*` |
| Raw CSV files | `s3://insurance-lakehouse-project/raw/` |
| Autoloader checkpoints | `s3://insurance-lakehouse-project/checkpoints/` |

---

## Lessons learned

**Serverless + external location** - direct S3 credentials do not work on Serverless compute. Unity Catalog external location is the correct pattern. The CloudFormation Quickstart wires everything automatically - IAM role, bucket policy, and Unity Catalog registration in one step.

**Schema hints are mandatory for Autoloader** - Autoloader infers CSV boolean values (`true`/`false`) as strings. Without explicit `cloudFiles.schemaHints`, every run throws `DELTA_MERGE_INCOMPATIBLE_DATATYPE` when writing to an existing Delta table with a boolean schema.

**Aggregate before join** - joining raw payments directly to claims multiplied rows because payments has multiple records per claim. Always reduce one-to-many tables to one row per grain before the final join.

**Window function over dropDuplicates** - `dropDuplicates` picks an arbitrary row when deduplicating. `row_number()` over `risk_score DESC` keeps the most meaningful record per claim in `fraud_indicators`.

**fillna scope matters** - applying `fillna(0)` to all columns corrupts string columns like `bundesland` and `agent_name`. Always scope `fillna` to numeric columns only.

**AWS region alignment** - CloudFormation stacks, S3 buckets, and Databricks workspaces must all be in the same region. A mismatch is silent and causes confusing access errors.

---

## Future improvements

- Scale to medium/large dataset mode (17M+ and 67M+ rows)
- Add incremental silver processing - currently full overwrite per run
- Train fraud detection model on `gold_claim_fraud_features` using Databricks MLflow
- Expand Lakeview dashboards with additional pages and filters
- Add data freshness monitoring and SLA alerting
- Implement schema evolution handling for new source fields
- Add dbt for gold layer transformations and documentation
