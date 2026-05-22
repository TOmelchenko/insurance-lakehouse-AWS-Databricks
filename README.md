# Insurance Lakehouse - AWS Databricks

End-to-end data engineering pipeline for a synthetic German insurance dataset. Built on Databricks Unity Catalog with AWS S3 as the storage backend, following a bronze-silver-gold medallion architecture.

---

## Project context

The fictional company is **Rheinland Versicherung AG**. The dataset covers customers, policies, claims, payments, agents, and fraud indicators across German federal states (Bundesländer). All data is synthetic and GDPR-aware.

---

## Architecture

```
Synthetic data generation (Databricks Serverless)
        ↓
S3 raw CSV files  (s3://insurance-lakehouse-project/raw/)
        ↓
Autoloader (cloudFiles)  - incremental ingestion with checkpoints
        ↓
Bronze layer      - raw arrival, ingestion metadata, no transformations
        ↓
Silver layer      - cleaned, validated, quarantined, PII-handled
        ↓
Gold layer        - KPIs, fraud analytics, AI-ready features
```

**Infrastructure:** Databricks Unity Catalog on AWS, S3 external location via CloudFormation, Serverless compute, Delta format throughout.

---

## Repository structure

```
notebooks/
  00_setup/
    00_project_setup
  01_data_generation/
    01_generate_synthetic_insurance_data    - generates temp views
    01_generate_synthetic_insurance_data_s3 - writes CSV files to S3
  02_bronze/
    02_bronze_ingestion                     - basic batch ingestion
    02_bronze_ingestion_s3                  - S3 batch ingestion
    02_bronze_ingestion_s3_autoloader       - Autoloader incremental ingestion
  03_silver/
    03_silver_customers
    03_silver_policies
    03_silver_claims
    03_silver_payments
    03_silver_agents
    03_silver_fraud_indicators
  04_gold/
    04_gold_claims_overview
    04_gold_policy_performance
    04_gold_customer_risk_profile
    04_gold_claims_payment_summary
    04_gold_fraud_risk_summary
    04_gold_agent_performance
    04_gold_claim_fraud_features
  05_dashboards/
    05_dashboard_views                      - creates SQL views on gold tables
    05_dashboard_queries                    - preview queries per view
  06_validation/
    06_day1_bronze_validation
    06_day2_silver_validation
    06_day3_gold_validation
    06_final_validation
  07_governance/
    07_governance_gdpr_final_design
  08_performance/
config/
  config.yml
  data_size_config.yml
  kpi_definitions.yml
  pii_config.yml
  project_config.yml
  quality_rules.yml
docs/
  data_dictionary.md
  data_quality_report.md
  gdpr_pii_handling.md
  kpi_definitions.md
  performance_notes.md
  final_project_summary.md
architecture/
  lakehouse_design.md
  s3_folder_design.md

S3 bucket: s3://insurance-lakehouse-project/
  raw/                       - source CSV files per dataset
  checkpoints/               - Autoloader checkpoint location per dataset
```

---

## Datasets

| Dataset | Description |
|---|---|
| customers | Customer demographics, consent, segment |
| policies | Insurance products, premium, coverage, status |
| claims | Claim events, amounts, types, fraud flag |
| payments | Settlement payments per claim |
| agents | Broker/agent profiles and commission rates |
| fraud_indicators | Risk scores and fraud flag features per claim |

---

## Bronze ingestion

Synthetic data is generated as CSV files and written directly to S3. Bronze ingestion uses **Databricks Autoloader** (`cloudFiles`) to load those files incrementally into Delta tables.

**How it works:**
- Autoloader monitors `s3://insurance-lakehouse-project/raw/{dataset}/`
- Checkpoints stored at `s3://insurance-lakehouse-project/checkpoints/{dataset}/`
- Each new file is processed exactly once - already-processed files are skipped
- `trigger(availableNow=True)` makes it run as a one-shot batch rather than a continuous stream

**Schema hints** are required for boolean fields that CSV reads as strings:

| Dataset | Boolean fields |
|---|---|
| customers | `gdpr_consent` |
| claims | `fraud_flag` |
| agents | `active_flag` |
| fraud_indicators | `suspicious_amount_flag`, `duplicate_claim_flag`, `late_report_flag`, `high_risk_region_flag` |

**Run modes:**

`FULL_RELOAD = True` - clears checkpoints and truncates bronze tables before ingesting. Used when regenerating all synthetic data from scratch.

`FULL_RELOAD = False` - incremental mode. Autoloader picks up only new files. Bronze tables accumulate data across runs.

**Audit columns added at ingestion:**
- `ingest_timestamp` - when the record was loaded
- `ingest_run_id` - UUID linking all records from the same pipeline run
- `source_file_name` - S3 path of the source file (`_metadata.file_path`)

---

## Silver layer

Each silver notebook reads from bronze, applies cleaning and validation, routes invalid records to quarantine, and writes trusted Delta tables.

**What silver does:**
- Trims and standardises text fields
- Casts dates and amounts to correct types
- Validates primary keys, foreign keys, and valid value lists
- Quarantines records that fail validation with a typed error reason
- Hashes and drops PII fields per `pii_config.yml`
- Enforces GDPR consent before any record reaches analytics

**Quarantine tables:** `quarantine_invalid_customers`, `quarantine_invalid_policies`, `quarantine_invalid_claims`, `quarantine_invalid_payments`

---

## Gold layer

| Table | Purpose | Grain |
|---|---|---|
| gold_claims_overview | Claims operations reporting | month + status + type + product + region |
| gold_policy_performance | Portfolio and premium analytics | product + status + channel + region |
| gold_customer_risk_profile | Customer-level risk summary | one row per customer |
| gold_claims_payment_summary | Claim settlement reporting | one row per claim |
| gold_fraud_risk_summary | Fraud monitoring by region and product | region + product + claim type + risk band |
| gold_agent_performance | Broker/agent KPIs | one row per agent |
| gold_claim_fraud_features | AI-ready fraud feature table | one row per claim |

**Core KPIs:**
- `total_claims` - count of claims
- `premium_revenue` - sum of premium amounts
- `claims_ratio` - total claim amount / premium revenue
- `fraud_risk_rate` - high risk claims / total claims

---

## Data quality rules

Defined in `quality_rules.yml` and enforced at the silver layer:

| Field | Valid values |
|---|---|
| `policy_status` | active, cancelled, expired |
| `policy_type` | car, home, health, travel, liability |
| `claim_status` | open, approved, rejected, under_review, paid |
| `payment_status` | paid, pending, rejected |
| `payment_method` | SEPA, bank_transfer, card |
| `risk_score` | 0-100 |

---

## PII and GDPR

Defined in `pii_config.yml`. Applied at the silver layer.

**Dropped at silver:** `first_name`, `last_name`, `email`, `phone_number`, `street`, `postal_code`

**Hashed (SHA-256):** `email` → `email_hash`, `phone_number` → `phone_hash`, `customer_id` → `customer_hash`

**Consent gate:** records where `gdpr_consent` is null are quarantined and excluded from all downstream tables.

**Gold rule:** no raw personal identifiers in any gold output. Aggregated or pseudonymised only.

---

## AWS - Databricks connection setup

This project uses a **Unity Catalog external location** to give Databricks Serverless compute access to S3. Direct credential-based access (`spark.conf.set`) does not work on Serverless - the external location approach is the correct and recommended method.

### Prerequisites

- Databricks workspace on AWS (created via AWS Marketplace)
- Unity Catalog metastore assigned to the workspace
- An S3 bucket in the same AWS region as your workspace (`us-east-2`)

### Step 1 - Enable Unity Catalog

1. Go to `https://accounts.cloud.databricks.com`
2. Click **Catalog** in the left sidebar
3. If no metastore exists for your region, click **Create metastore**
4. Set the region to match your workspace (e.g. `us-east-2`)
5. Go to **Workspaces**, click your workspace, and assign the metastore

### Step 2 - Create the external location via Quickstart

This is the easiest method - Databricks generates a CloudFormation stack that wires everything up automatically.

1. Open your Databricks workspace
2. Click **Catalog** in the left sidebar
3. Click the **gear icon** at the top of the Catalog panel
4. Click **External Locations**
5. Click **Create external location > Quickstart**
6. Fill in your S3 bucket name: `s3://your-bucket-name`
7. Copy the pre-generated **Personal Access Token**
8. Click the button to proceed to AWS Console

### Step 3 - Create the CloudFormation stack

AWS Console opens with a pre-filled CloudFormation form:

1. Paste your Personal Access Token into the **Databricks Personal Access Token** field
2. Confirm the bucket name and workspace URL are correct
3. Scroll to the bottom and click **Create stack**
4. Wait 1-2 minutes for the stack to complete

The CloudFormation stack automatically creates:
- An IAM role with S3 read/write permissions
- An IAM instance profile
- A bucket policy on your S3 bucket
- The external location registration in Unity Catalog

> **Important:** Make sure you are in the correct AWS region when creating the stack. The stack must be in the same region as your S3 bucket and Databricks workspace.

### Step 4 - Validate the external location

1. Go back to your Databricks workspace
2. Catalog > gear icon > **External Locations**
3. Click your external location
4. Click **Test connection**

All checks should pass: Read, Write, Delete, Assume Role, Self Assume Role, External ID Condition.

### Step 5 - Verify S3 access from a notebook

Once the external location is set up, Serverless compute accesses S3 automatically with no credentials in the notebook:

```python
# test write
df = spark.createDataFrame([("test",)], ["value"])
df.write.mode("overwrite").format("parquet").save("s3://your-bucket-name/test/")
print("done")

# test read
dbutils.fs.ls("s3://your-bucket-name/")
```

### Why this works

Serverless compute authenticates through Unity Catalog, not through Spark config or environment variables. When Serverless sees an `s3://` path that matches a registered external location, it automatically uses the IAM role attached to that location. No access keys, no secrets manager, no `spark.conf.set` needed.

The external location is the link between the S3 path and the IAM role that has permission to access it.

---

## Tech stack

- **Compute:** Databricks Serverless + Unity Catalog
- **Storage:** AWS S3 (external location set up via AWS CloudFormation)
- **Ingestion:** Databricks Autoloader (`cloudFiles`) with S3 checkpoints
- **Format:** Delta Lake throughout
- **Language:** PySpark (Python 3)
- **Config:** YAML for quality rules and PII definitions
