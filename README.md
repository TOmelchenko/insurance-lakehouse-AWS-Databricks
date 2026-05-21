# Insurance Lakehouse - AWS Databricks

End-to-end data engineering pipeline for a synthetic German insurance dataset. Built on Databricks Unity Catalog with AWS S3 as the storage backend, following a bronze-silver-gold medallion architecture.

---

## Project context

The fictional company is **Rheinland Versicherung AG**. The dataset covers customers, policies, claims, payments, agents, and fraud indicators across German federal states (Bundesländer). All data is synthetic and GDPR-aware.

---

## Architecture

```
Raw synthetic data
      ↓
  Bronze layer      - raw arrival, ingestion metadata, no transformations
      ↓
  Silver layer      - cleaned, validated, quarantined, PII-handled
      ↓
  Gold layer        - KPIs, fraud analytics, AI-ready features
```

**Infrastructure:** Databricks Unity Catalog on AWS, S3 managed storage, Serverless compute, Delta format throughout.

---

## Repository structure

```
notebooks/
  01_project_setup/
  02_generate_data/         - synthetic data generation
  03_silver/
    09_silver_customers
    10_silver_policies
    11_silver_claims
    12_silver_payments
    13_silver_agents
    14_silver_fraud_indicators
  05_quality/
    15_data_quality_summary
    16_quarantine_review
  07_governance/
    17_gdpr_pii_handling
  gold/
    15_gold_claims_overview
    16_gold_policy_performance
    17_gold_customer_risk_profile
    18_gold_claims_payment_summary
    19_gold_fraud_risk_summary
    20_gold_agent_performance
    21_gold_claim_fraud_features
config/
  quality_rules.yml          - valid values per field
  pii_config.yml             - PII fields, hash fields, gold exclusions
docs/
  day2_gdpr_pii_handling.md
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

## Tech stack

- **Compute:** Databricks Serverless + Unity Catalog
- **Storage:** AWS S3 (managed via Unity Catalog external location)
- **Format:** Delta Lake throughout
- **Language:** PySpark (Python 3)
- **Config:** YAML for quality rules and PII definitions
