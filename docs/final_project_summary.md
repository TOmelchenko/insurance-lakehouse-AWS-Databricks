# Final Project Summary
## Insurance Lakehouse - Rheinland Versicherung AG

---

## What was built

An end-to-end data engineering pipeline for a synthetic German insurance company, built on Databricks Unity Catalog with AWS S3 as the storage backend. The pipeline covers data generation, ingestion, cleaning, validation, analytics, and governance across a full bronze-silver-gold medallion architecture.

---

## Dataset

Synthetic data for a fictional German insurer covering 6 datasets in small mode:

| Dataset | Rows |
|---|---|
| customers | 10,000 |
| policies | 25,000 |
| claims | 50,000 |
| payments | 50,000 |
| agents | 1,000 |
| fraud_indicators | 50,000 |

The generator supports test, small, medium, and large modes scaling up to 47M+ rows.

---

## Pipeline layers

### Bronze - raw ingestion

- Synthetic CSV data written to S3 (`s3://insurance-lakehouse-project/raw/`)
- Databricks Autoloader (`cloudFiles`) reads files incrementally
- Checkpoints stored in S3 prevent reprocessing already-loaded files
- Schema hints applied for boolean fields that CSV reads as strings
- Audit columns added: `ingest_timestamp`, `ingest_run_id`, `source_file_name`
- Supports full reload (truncate + clear checkpoints) and incremental modes

### Silver - trusted data

- Cleaning: text trimming, date casting, status normalisation
- Validation: primary keys, foreign keys, valid value lists from `quality_rules.yml`
- Quarantine: invalid records preserved with typed error reasons and severity
- PII handling: fields dropped and hashed per `pii_config.yml`
- GDPR consent enforced - non-consenting customers excluded from all downstream tables

**Silver validation results:**
- 22,506 payment records quarantined - `payment_date_before_claim_date`
- 18,306 fraud indicator records deduplicated by design - highest risk_score kept per claim

### Gold - analytics

7 Gold tables built for business reporting and AI feature engineering:

| Table | Purpose |
|---|---|
| gold_claims_overview | Monthly claims operations reporting |
| gold_policy_performance | Portfolio and premium analytics |
| gold_customer_risk_profile | One row per customer with risk metrics |
| gold_claims_payment_summary | Claim settlement and payment behaviour |
| gold_fraud_risk_summary | Fraud monitoring by region and product |
| gold_agent_performance | Broker and agent KPIs |
| gold_claim_fraud_features | AI-ready feature table, one row per claim |

All one-row-per-entity tables (customer, agent, claim) validated for zero duplicate grain.

---

## Key engineering decisions

**Autoloader over batch read** - incremental by design, processes only new files, checkpoints survive restarts.

**Schema hints for booleans** - CSV always reads booleans as strings; hints tell Autoloader the correct type upfront, avoiding Delta schema merge errors.

**Aggregate before join** - payments aggregated to one row per claim before joining to claims table, preventing grain duplication.

**Window function deduplication** - fraud indicators deduplicated using `row_number()` over `risk_score` descending, keeping the most significant record per claim rather than an arbitrary row.

**Anti-join for foreign keys** - `left_anti` join used for FK validation across policies, claims, and payments, routing orphaned records to quarantine.

**`fillna` scoped to numeric columns** - prevents string columns like `bundesland` and `agent_name` being overwritten with `0`.

**Unity Catalog external location** - S3 access configured via CloudFormation-generated IAM role registered as a Unity Catalog external location. Serverless compute authenticates automatically - no credentials in notebooks.

---

## GDPR compliance

- PII fields identified in `pii_config.yml`
- Direct identifiers dropped at silver: `first_name`, `last_name`, `email`, `phone_number`, `street`, `postal_code`
- Quasi-identifiers hashed using SHA-256: `email_hash`, `phone_hash`, `customer_hash`
- Consent gate enforced at silver - records with null `gdpr_consent` quarantined
- Gold outputs contain no raw personal identifiers
- Role-based access designed across bronze (engineers), silver (analysts), gold (business users)
- All records traceable via `ingest_run_id` supporting GDPR Article 30

---

## Infrastructure

| Component | Technology |
|---|---|
| Compute | Databricks Serverless |
| Storage | AWS S3 |
| Catalog | Databricks Unity Catalog |
| S3 access | Unity Catalog external location via AWS CloudFormation |
| Table format | Delta Lake |
| Ingestion | Databricks Autoloader |
| Language | PySpark (Python 3) |
| Configuration | YAML (quality_rules.yml, pii_config.yml) |

---

## Repository structure

```
notebooks/
  00_setup/
  01_data_generation/
  02_bronze/
  03_silver/
  04_gold/
  05_dashboards/
  06_validation/
  07_governance/
  08_performance/
config/
  quality_rules.yml
  pii_config.yml
  kpi_definitions.yml
docs/
  data_dictionary.md
  kpi_definitions.md
  data_quality_report.md
  gdpr_pii_handling.md
  performance_notes.md
  final_project_summary.md
architecture/
  lakehouse_design.md
  s3_folder_design.md
```
