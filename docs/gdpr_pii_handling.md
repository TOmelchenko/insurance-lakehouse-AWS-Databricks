# PII Handling and Data Governance
## Insurance Lakehouse - AWS Databricks Project

---

## Overview

This project processes personal data for German insurance customers and falls under GDPR (EU 2016/679). The pipeline uses a bronze-silver-gold medallion architecture on Databricks Unity Catalog with AWS S3 as the underlying storage layer. PII handling, field masking, and consent enforcement are applied at the silver layer - raw data never leaves the bronze layer in an unmasked state.

---

## PII Fields by Dataset

### Customers

| Field | PII Type | Sensitivity | Treatment |
|---|---|---|---|
| `first_name` | Direct identifier | HIGH | Dropped at silver (`pii_config.yml`) |
| `last_name` | Direct identifier | HIGH | Dropped at silver (`pii_config.yml`) |
| `email` | Direct identifier | HIGH | Hashed (SHA-256), original dropped at silver |
| `phone_number` | Direct identifier | HIGH | Hashed (SHA-256), original dropped at silver |
| `street` | Direct identifier | HIGH | Dropped at silver |
| `postal_code` | Quasi-identifier | MEDIUM | Dropped at silver (`pii_config.yml`) |
| `date_of_birth` | Quasi-identifier | MEDIUM | Retained, used to derive `customer_age` |
| `customer_id` | Internal identifier | LOW | Hashed for traceability |
| `city` | Quasi-identifier | LOW | Retained, normalized |
| `bundesland` | Quasi-identifier | LOW | Retained |
| `gdpr_consent` | Consent flag | HIGH | Required - records without consent are quarantined |

### Policies

| Field | PII Type | Sensitivity | Treatment |
|---|---|---|---|
| `policy_id` | Internal identifier | LOW | Hashed for traceability |
| `customer_id` | Foreign key to PII | MEDIUM | Retained for joins |
| `premium_amount` | Financial data | MEDIUM | Retained |
| `coverage_amount` | Financial data | MEDIUM | Retained |

### Claims

| Field | PII Type | Sensitivity | Treatment |
|---|---|---|---|
| `customer_id` | Foreign key to PII | MEDIUM | Retained for joins |
| `claim_description` | Free text, may contain PII | MEDIUM | Retained, flagged for manual review |
| `fraud_flag` | Sensitive classification | HIGH | Retained, access restricted |

### Payments

| Field | PII Type | Sensitivity | Treatment |
|---|---|---|---|
| `iban_hash` | Financial identifier | HIGH | Pre-hashed at source generation |
| `payment_amount` | Financial data | MEDIUM | Retained |

### Agents

| Field | PII Type | Sensitivity | Treatment |
|---|---|---|---|
| `agent_name` | Direct identifier | MEDIUM | Retained, normalized |
| `commission_rate` | Financial data | MEDIUM | Retained |

### Fraud Indicators

| Field | PII Type | Sensitivity | Treatment |
|---|---|---|---|
| `risk_score` | Derived classification | HIGH | Retained, access restricted |
| `risk_category` | Derived classification | HIGH | Derived at silver (HIGH/MEDIUM/LOW) |

---

## Hashing Strategy

All hashing uses **SHA-256** via `F.sha2(col, 256)`. This is a one-way hash - original values cannot be recovered.

```python
# applied at silver layer for customers
.withColumn("customer_hash", F.sha2(F.col("customer_id").cast("string"), 256))
.withColumn("email_hash",    F.sha2(F.lower(F.trim(F.col("email"))), 256))
.withColumn("phone_hash",    F.sha2(F.trim(F.col("phone_number")), 256))
```

**Normalisation before hashing:** email is lowercased and trimmed, phone is trimmed. This ensures the same input always produces the same hash regardless of formatting differences.

**What is hashed vs dropped:**

- Hashed: fields needed for traceability or joining (`customer_id`, `email`, `phone_number`)
- Dropped entirely: fields with no analytical value after masking (`street`)

---

## Masking and Field Removal

At the silver layer, the following fields are permanently removed from the `silver_customers` table:

```python
.drop("first_name", "last_name", "email", "phone_number", "street", "postal_code")
```

These fields remain in `bronze_customers` which is access-restricted. Analysts working on silver and gold layers never see raw PII.

Fields dropped per `pii_config.yml` (`exclude_from_gold`): `first_name`, `last_name`, `email`, `phone_number`, `street`, `postal_code`.

---

## Consent-Aware Analytics

GDPR consent is enforced at the silver ingestion step. Any customer record where `gdpr_consent` is `NULL` or `FALSE` is routed to quarantine and excluded from all downstream analytics.

```python
# only consenting customers reach silver
valid_customers = customers_prepared.filter(
    F.col("customer_id").isNotNull() &
    F.col("gdpr_consent").isNotNull()
)
```

This means:
- Gold layer aggregations only include consenting customers
- Fraud scoring, claim analytics, and agent reporting are all consent-gated through the customer join
- Non-consenting records are preserved in quarantine for audit purposes but never used in reporting

---

## Role-Based Access Design

Unity Catalog enforces access at the schema level. Three access tiers are defined:

### Tier 1 - Bronze (restricted)
- Contains raw, unmasked PII
- Access: data engineers only
- Purpose: debugging, reprocessing, audit trail

```sql
GRANT SELECT ON SCHEMA insurance_lakehouse.bronze TO `data-engineers`;
```

### Tier 2 - Silver (internal analysts)
- PII hashed or dropped
- Access: data analysts, data scientists
- Purpose: building models, segmentation, reporting

```sql
GRANT SELECT ON SCHEMA insurance_lakehouse.silver TO `data-analysts`;
GRANT SELECT ON SCHEMA insurance_lakehouse.silver TO `data-scientists`;
```

### Tier 3 - Gold (business users)
- Aggregated, no individual-level PII
- Access: business stakeholders, BI tools
- Purpose: dashboards, KPIs, executive reporting

```sql
GRANT SELECT ON SCHEMA insurance_lakehouse.gold TO `business-users`;
```

### Quarantine (audit only)
- Contains rejected records with error metadata
- Access: data engineers and compliance team only

```sql
GRANT SELECT ON SCHEMA insurance_lakehouse.quarantine TO `data-engineers`;
GRANT SELECT ON SCHEMA insurance_lakehouse.quarantine TO `compliance-team`;
```

---

## Audit Metadata

Every bronze and silver table carries three audit columns added at ingestion time:

| Column | Type | Purpose |
|---|---|---|
| `ingest_timestamp` | timestamp | When the record was ingested |
| `ingest_run_id` | string (UUID) | Links all records from the same pipeline run |
| `source_file_name` | string | Source table or file path |

These columns support GDPR Article 30 (records of processing activities) and make it possible to trace any record back to its origin.

---

## Quarantine and Error Handling

Invalid records are never silently dropped. They are routed to dedicated quarantine tables with full error context:

| Column | Purpose |
|---|---|
| `record_id` | Primary key of the rejected record |
| `source_table` | Which bronze table it came from |
| `error_reason` | Specific validation failure |
| `error_severity` | HIGH / MEDIUM / LOW |
| `quarantine_timestamp` | When it was quarantined |
| `original_record_json` | Full original record as JSON for audit |

Error categories include:
- `missing_customer_id` - record cannot be linked to a customer
- `invalid_gdpr_consent` - consent not confirmed, GDPR requirement
- `invalid_premium_amount` - negative or null financial value
- `coverage_not_greater_than_premium` - coverage must exceed premium
- `missing_claim_date` - incomplete claim record
- `invalid_claim_amount` - claim amount must be greater than 0
- `invalid_risk_score` - score outside 0-100 range
- `invalid_policy_status` - value not in `[active, cancelled, expired]`
- `invalid_policy_type` - value not in `[car, home, health, travel, liability]`
- `invalid_claim_status` - value not in `[open, approved, rejected, under_review, paid]`
- `invalid_payment_status` - value not in `[paid, pending, rejected]`
- `invalid_payment_method` - value not in `[SEPA, bank_transfer, card]`
- `customer_id_not_in_silver_customers` - foreign key violation
- `policy_id_not_in_silver_policies` - foreign key violation
- `claim_id_not_in_silver_claims` - foreign key violation
- `payment_date_before_claim_date` - business logic violation

---

## Foreign Key Validation

Silver layer enforces referential integrity across datasets. A record that cannot be linked to a trusted parent is not analytically reliable and is routed to quarantine.

| Dataset | Foreign Key | Must Exist In | Quarantine Reason |
|---|---|---|---|
| Policies | `customer_id` | `silver_customers` | `customer_id_not_in_silver_customers` |
| Claims | `policy_id` | `silver_policies` | `policy_id_not_in_silver_policies` |
| Claims | `customer_id` | `silver_customers` | `customer_id_not_in_silver_customers` |
| Payments | `claim_id` | `silver_claims` | `claim_id_not_in_silver_claims` |
| Payments | `payment_date` | `silver_claims.claim_date` | `payment_date_before_claim_date` |

Implementation uses Spark `left_anti` joins - records that fail the join are captured and routed to quarantine with full error context, not silently dropped.

```python
# example - policies with no matching customer
invalid_fk = field_valid.join(
    silver_customers.select("customer_id"),
    on="customer_id",
    how="left_anti"
)
```

---

## Gold Exposure Policy

Gold outputs are the final consumer-facing layer and must never expose individual-level personal data. The following rules apply to all Gold tables and dashboards.

**What is allowed in Gold:**
- Aggregated counts, sums, averages grouped by region, product, or time period
- Anonymized risk scores and fraud summaries without individual identifiers
- Agent performance KPIs without customer-level detail
- Derived fields such as `customer_age`, `policy_duration_days`, `risk_category`
- Hashed identifiers (`customer_hash`, `email_hash`) only when needed for joining - never for display

**What is never allowed in Gold:**
- `first_name`, `last_name`, `email`, `phone_number`, `street`, `postal_code` - defined in `pii_config.yml` as `exclude_from_gold`
- Raw `customer_id` linked to personal attributes
- Individual claim or payment records without aggregation
- Any field that could re-identify a customer when combined with other fields

**Consent gate:**
All Gold outputs that involve customer data must be derived from `silver_customers` which already excludes non-consenting records. No additional consent filtering is required at Gold if the Silver join is used correctly.

---

## Data Quality Rules

Valid value constraints are defined in `quality_rules.yml` and enforced at the silver layer. Records with values outside these lists are routed to quarantine.

| Dataset | Field | Valid Values |
|---|---|---|
| Policies | `policy_status` | `active`, `cancelled`, `expired` |
| Policies | `policy_type` | `car`, `home`, `health`, `travel`, `liability` |
| Claims | `claim_status` | `open`, `approved`, `rejected`, `under_review`, `paid` |
| Payments | `payment_status` | `paid`, `pending`, `rejected` |
| Payments | `payment_method` | `SEPA`, `bank_transfer`, `card` |
| Fraud Indicators | `risk_score` | 0-100 (min/max range) |

All string fields are lowercased and trimmed before validation to avoid case-sensitivity failures.

---

## GDPR Compliance Summary

| GDPR Requirement | Implementation |
|---|---|
| Article 5 - Data minimisation | PII fields dropped or hashed at silver layer |
| Article 6 - Lawful basis | `gdpr_consent` field enforced before analytics |
| Article 25 - Privacy by design | Masking applied at pipeline level, not ad hoc |
| Article 30 - Records of processing | Audit metadata on every table |
| Article 32 - Security | Unity Catalog role-based access, no raw PII in analytics layer |
| Article 17 - Right to erasure | Records traceable via `customer_hash` and `ingest_run_id` |
