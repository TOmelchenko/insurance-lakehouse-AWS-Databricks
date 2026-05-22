# Data Dictionary
## Insurance Lakehouse - Rheinland Versicherung AG

---

## Bronze layer

### bronze_customers

| Column | Type | Description |
|---|---|---|
| `customer_id` | string | Unique customer identifier |
| `first_name` | string | Customer first name (dropped at silver) |
| `last_name` | string | Customer last name (dropped at silver) |
| `date_of_birth` | date | Date of birth |
| `gender` | string | Customer gender |
| `email` | string | Email address (hashed at silver) |
| `phone_number` | string | Phone number (hashed at silver) |
| `street` | string | Street address (dropped at silver) |
| `city` | string | City |
| `postal_code` | string | Postal code (dropped at silver) |
| `bundesland` | string | German federal state |
| `country` | string | Country |
| `registration_date` | date | Date customer registered |
| `gdpr_consent` | boolean | Whether customer has given GDPR consent |
| `customer_segment` | string | Customer segment classification |
| `ingest_timestamp` | timestamp | When record was ingested |
| `ingest_run_id` | string | UUID of the ingestion run |
| `source_file_name` | string | S3 path of source CSV file |

### bronze_policies

| Column | Type | Description |
|---|---|---|
| `policy_id` | string | Unique policy identifier |
| `customer_id` | string | Foreign key to customers |
| `policy_type` | string | Type of insurance (car, home, health, travel, liability) |
| `start_date` | date | Policy start date |
| `end_date` | date | Policy end date |
| `premium_amount` | double | Annual premium amount in EUR |
| `coverage_amount` | double | Maximum coverage amount in EUR |
| `policy_status` | string | Status (active, cancelled, expired) |
| `agent_id` | string | Foreign key to agents |
| `sales_channel` | string | How the policy was sold |
| `created_at` | timestamp | Record creation timestamp |
| `updated_at` | timestamp | Record last update timestamp |
| `ingest_timestamp` | timestamp | When record was ingested |
| `ingest_run_id` | string | UUID of the ingestion run |
| `source_file_name` | string | S3 path of source CSV file |

### bronze_claims

| Column | Type | Description |
|---|---|---|
| `claim_id` | string | Unique claim identifier |
| `policy_id` | string | Foreign key to policies |
| `customer_id` | string | Foreign key to customers |
| `claim_date` | date | Date claim was filed |
| `claim_type` | string | Type of claim |
| `claim_amount` | double | Amount claimed in EUR |
| `claim_status` | string | Status (open, approved, rejected, under_review, paid) |
| `claim_description` | string | Free text description |
| `reported_channel` | string | How the claim was reported |
| `fraud_flag` | boolean | Whether claim is flagged for fraud |
| `created_at` | timestamp | Record creation timestamp |
| `ingest_timestamp` | timestamp | When record was ingested |
| `ingest_run_id` | string | UUID of the ingestion run |
| `source_file_name` | string | S3 path of source CSV file |

### bronze_payments

| Column | Type | Description |
|---|---|---|
| `payment_id` | string | Unique payment identifier |
| `claim_id` | string | Foreign key to claims |
| `payment_date` | date | Date payment was made |
| `payment_amount` | double | Amount paid in EUR |
| `payment_status` | string | Status (paid, pending, rejected) |
| `payment_method` | string | Payment method (SEPA, bank_transfer, card) |
| `iban_hash` | string | Hashed IBAN number |
| `created_at` | timestamp | Record creation timestamp |
| `ingest_timestamp` | timestamp | When record was ingested |
| `ingest_run_id` | string | UUID of the ingestion run |
| `source_file_name` | string | S3 path of source CSV file |

### bronze_agents

| Column | Type | Description |
|---|---|---|
| `agent_id` | string | Unique agent identifier |
| `agent_name` | string | Agent full name |
| `region` | string | Sales region |
| `city` | string | Agent city |
| `bundesland` | string | German federal state |
| `commission_rate` | double | Commission rate (0.0 to 1.0) |
| `active_flag` | boolean | Whether agent is currently active |
| `ingest_timestamp` | timestamp | When record was ingested |
| `ingest_run_id` | string | UUID of the ingestion run |
| `source_file_name` | string | S3 path of source CSV file |

### bronze_fraud_indicators

| Column | Type | Description |
|---|---|---|
| `claim_id` | string | Foreign key to claims |
| `previous_claims_count` | integer | Number of prior claims by this customer |
| `suspicious_amount_flag` | boolean | Amount is unusually high |
| `duplicate_claim_flag` | boolean | Possible duplicate of another claim |
| `late_report_flag` | boolean | Claim reported significantly after incident |
| `high_risk_region_flag` | boolean | Claim originates from high-risk region |
| `risk_score` | integer | Overall fraud risk score (0-100) |
| `ingest_timestamp` | timestamp | When record was ingested |
| `ingest_run_id` | string | UUID of the ingestion run |
| `source_file_name` | string | S3 path of source CSV file |

---

## Silver layer

Silver tables contain the same fields as bronze with the following differences:

- PII fields dropped: `first_name`, `last_name`, `email`, `phone_number`, `street`, `postal_code`
- Hash fields added: `customer_hash`, `email_hash`, `phone_hash`, `policy_hash`, `claim_hash`, `payment_hash`, `agent_hash`
- Derived fields added per table:

| Table | Derived field | Formula |
|---|---|---|
| silver_customers | `customer_age` | `floor(months_between(current_date, date_of_birth) / 12)` |
| silver_policies | `policy_duration_days` | `datediff(end_date, start_date)` |
| silver_fraud_indicators | `risk_category` | HIGH / MEDIUM / LOW from risk_score |
| silver_fraud_indicators | `fraud_indicator_count` | Sum of all boolean fraud flags |

---

## Gold layer

### gold_claims_overview

Grain: `claim_month + claim_status + claim_type + policy_type + bundesland`

| Column | Description |
|---|---|
| `claim_month` | Month of claim truncated to first day |
| `total_claims` | Count of claims in group |
| `open/approved/rejected/paid_claims` | Count per status |
| `total_claim_amount` | Sum of claim amounts |
| `avg_claim_amount` | Average claim amount |
| `avg_risk_score` | Average fraud risk score |
| `fraud_flag_rate` | Share of claims with fraud flag |

### gold_policy_performance

Grain: `policy_type + policy_status + sales_channel + bundesland`

| Column | Description |
|---|---|
| `total_policies` | Count of policies |
| `active_policies` | Count of active policies |
| `cancelled_policies` | Count of cancelled policies |
| `premium_revenue` | Sum of premiums |
| `avg_premium` | Average premium |
| `total_coverage` | Sum of coverage amounts |

### gold_customer_risk_profile

Grain: one row per `customer_id`

| Column | Description |
|---|---|
| `policy_count` | Distinct policies held |
| `total_premium_amount` | Total premiums paid |
| `claim_count` | Distinct claims filed |
| `total_claim_amount` | Total amount claimed |
| `avg_risk_score` | Average fraud risk score |
| `high_risk_claims` | Claims with risk_score >= 70 |
| `gdpr_consent` | Customer consent status |

### gold_claims_payment_summary

Grain: one row per `claim_id`

| Column | Description |
|---|---|
| `payment_count` | Number of payment records |
| `total_paid_amount` | Total amount paid |
| `payment_rejection_count` | Number of rejected payments |
| `first_payment_date` | Date of first payment |
| `last_payment_date` | Date of most recent payment |
| `payment_delay_days` | Days from claim date to first payment |
| `claim_to_payment_ratio` | Paid amount / claim amount |

### gold_fraud_risk_summary

Grain: `bundesland + policy_type + claim_type + risk_band`

| Column | Description |
|---|---|
| `total_claims` | Count of claims in group |
| `high_risk_claims` | Claims with risk_band = high |
| `fraud_risk_rate` | high_risk_claims / total_claims |
| `avg_risk_score` | Average risk score |
| `suspicious_amount_count` | Count of suspicious amount flags |
| `duplicate_claim_count` | Count of duplicate claim flags |
| `late_report_count` | Count of late report flags |

### gold_agent_performance

Grain: one row per `agent_id`

| Column | Description |
|---|---|
| `total_policies_sold` | Distinct policies sold |
| `active_policies` | Active policies in portfolio |
| `premium_revenue` | Total premium revenue |
| `total_claims_linked` | Claims on agent policies |
| `total_claim_amount` | Total claim value |
| `total_paid_amount` | Total payments made |
| `claims_ratio` | total_claim_amount / premium_revenue |
| `estimated_commission` | premium_revenue * commission_rate |

### gold_claim_fraud_features

Grain: one row per `claim_id` - AI-ready feature table

| Column | Description |
|---|---|
| `claim_amount_to_coverage_ratio` | claim_amount / coverage_amount |
| `policy_age_days` | Days from policy start to claim date |
| `customer_age` | Customer age at claim date |
| `payment_delay_days` | Days from claim date to first payment |
| `previous_claims_count` | Prior claims by customer |
| `suspicious_amount_flag` | Fraud indicator |
| `duplicate_claim_flag` | Fraud indicator |
| `late_report_flag` | Fraud indicator |
| `high_risk_region_flag` | Fraud indicator |
| `risk_score` | Overall fraud risk score (0-100) |
| `risk_category` | HIGH / MEDIUM / LOW |
