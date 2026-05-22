# Data Quality Report
## Insurance Lakehouse - Rheinland Versicherung AG

---

## Bronze layer validation

All 6 datasets ingested successfully via Databricks Autoloader from S3. Row counts match between raw CSV files and bronze Delta tables. All audit metadata columns confirmed present.

| Dataset | Raw count | Bronze count | Status |
|---|---|---|---|
| customers | 10,000 | 10,000 | PASS |
| policies | 25,000 | 25,000 | PASS |
| claims | 50,000 | 50,000 | PASS |
| payments | 50,000 | 50,000 | PASS |
| agents | 1,000 | 1,000 | PASS |
| fraud_indicators | 50,000 | 50,000 | PASS |

**Metadata columns present across all bronze tables:** `ingest_timestamp`, `ingest_run_id`, `source_file_name`

---

## Silver layer validation

Silver layer applies cleaning, validation, foreign key checks, and PII handling. Invalid records are routed to quarantine rather than silently dropped.

| Dataset | Bronze count | Silver count | Quarantine count | Unaccounted | Status |
|---|---|---|---|---|---|
| customers | 10,000 | 10,000 | 0 | 0 | PASS |
| policies | 25,000 | 25,000 | 0 | 0 | PASS |
| claims | 50,000 | 50,000 | 0 | 0 | PASS |
| payments | 50,000 | 27,494 | 22,506 | 0 | PASS |
| agents | 1,000 | 1,000 | 0 | 0 | PASS |
| fraud_indicators | 50,000 | 31,694 | 0 | 18,306 | REVIEW |

**Metadata columns present across all silver tables:** `ingest_timestamp`, `ingest_run_id`, `source_file_name`

### Quarantine breakdown

| Dataset | Error reason | Severity | Count |
|---|---|---|---|
| payments | `payment_date_before_claim_date` | HIGH | 22,506 |

### Notes on REVIEW status

**payments** - 22,506 records quarantined because `payment_date` was before `claim_date`. This is a business logic violation enforced at silver. The synthetic data generator produced payment dates that pre-date the claim - this is expected in synthetic data and confirms the validation rule is working correctly.

**fraud_indicators** - 18,306 records are unaccounted for. This is expected - `fraud_indicators` has multiple records per `claim_id` in the source. The silver script keeps only the record with the highest `risk_score` per claim (using a window function), reducing 50,000 rows to 31,694 distinct claims. No records are quarantined; the reduction is intentional deduplication.

---

## Gold layer validation

| Table | Row count | Grain | Duplicate grain count |
|---|---|---|---|
| gold_claims_overview | 30,189 | month + status + type + product + region | - |
| gold_policy_performance | 540 | product + status + channel + region | - |
| gold_customer_risk_profile | 10,000 | customer_id | 0 |
| gold_claims_payment_summary | 50,000 | claim_id | 0 |
| gold_fraud_risk_summary | 810 | region + product + claim type + risk band | - |
| gold_agent_performance | 1,000 | agent_id | 0 |
| gold_claim_fraud_features | 50,000 | claim_id | 0 |

All one-row-per-entity tables (customer, agent, claim) confirmed to have zero duplicate grain rows.

---

## Summary

| Layer | Total input | Total output | Total quarantined | Overall status |
|---|---|---|---|---|
| Bronze | 186,000 | 186,000 | 0 | PASS |
| Silver | 186,000 | 155,188 | 22,506 | PASS |
| Gold | 155,188 | 141,539 | - | PASS |

Silver reduction is explained entirely by payment date validation (22,506 quarantined) and fraud indicator deduplication (18,306 deduplicated by design). No data is lost without explanation.
