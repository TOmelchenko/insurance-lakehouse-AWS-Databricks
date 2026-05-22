# KPI Definitions
## Insurance Lakehouse - Rheinland Versicherung AG

---

## Core KPIs

| KPI | Formula | Source table | Description |
|---|---|---|---|
| `total_claims` | `count(*)` | gold_claims_overview | Total number of claims |
| `premium_revenue` | `sum(premium_amount)` | gold_policy_performance | Total premium income across all policies |
| `claims_ratio` | `total_claim_amount / premium_revenue` | gold_agent_performance | Measures how much of premium revenue is consumed by claims. A ratio above 1.0 means claims exceed premiums collected. |
| `fraud_risk_rate` | `high_risk_claims / total_claims` | gold_fraud_risk_summary | Share of claims classified as high risk. Higher rate indicates elevated fraud exposure. |

---

## Claims KPIs

| KPI | Formula | Description |
|---|---|---|
| `total_claim_amount` | `sum(claim_amount)` | Total value of all claims |
| `avg_claim_amount` | `avg(claim_amount)` | Average claim size |
| `open_claims` | `sum(claim_status = 'open')` | Claims not yet resolved |
| `approved_claims` | `sum(claim_status = 'approved')` | Claims approved for payment |
| `rejected_claims` | `sum(claim_status = 'rejected')` | Claims denied |
| `paid_claims` | `sum(claim_status = 'paid')` | Claims fully settled |
| `fraud_flag_rate` | `avg(fraud_flag.cast(int))` | Share of claims with fraud flag set |
| `avg_risk_score` | `avg(risk_score)` | Average fraud risk score (0-100) |

---

## Policy KPIs

| KPI | Formula | Description |
|---|---|---|
| `total_policies` | `count(policy_id)` | Total number of policies |
| `active_policies` | `sum(policy_status = 'active')` | Policies currently in force |
| `cancelled_policies` | `sum(policy_status = 'cancelled')` | Policies cancelled before end date |
| `avg_premium` | `avg(premium_amount)` | Average premium per policy |
| `total_coverage` | `sum(coverage_amount)` | Total insurance exposure |

---

## Payment KPIs

| KPI | Formula | Description |
|---|---|---|
| `total_paid_amount` | `sum(payment_amount)` | Total amount paid out for a claim |
| `payment_count` | `count(*)` | Number of payment records per claim |
| `payment_rejection_count` | `sum(payment_status = 'rejected')` | Number of rejected payment attempts |
| `payment_delay_days` | `datediff(first_payment_date, claim_date)` | Days from claim filing to first payment. Negative values indicate a data issue. |
| `claim_to_payment_ratio` | `total_paid_amount / claim_amount` | How much of the claimed amount was actually paid. Values above 1.0 indicate overpayment. |

---

## Fraud KPIs

| KPI | Formula | Description |
|---|---|---|
| `high_risk_claims` | `sum(risk_score >= 70)` | Claims with high fraud risk |
| `fraud_risk_rate` | `high_risk_claims / total_claims` | Share of high-risk claims |
| `avg_risk_score` | `avg(risk_score)` | Average fraud risk score per group |
| `suspicious_amount_count` | `sum(suspicious_amount_flag = true)` | Claims flagged for suspicious amounts |
| `duplicate_claim_count` | `sum(duplicate_claim_flag = true)` | Claims flagged as potential duplicates |
| `late_report_count` | `sum(late_report_flag = true)` | Claims reported later than expected |

---

## Agent KPIs

| KPI | Formula | Description |
|---|---|---|
| `total_policies_sold` | `countDistinct(policy_id)` | Number of distinct policies sold by agent |
| `active_policies` | `sum(policy_status = 'active')` | Active policies in agent's portfolio |
| `total_claims_linked` | `count(claim_id)` | Claims on policies sold by this agent |
| `claims_ratio` | `total_claim_amount / premium_revenue` | Agent-level loss ratio |
| `estimated_commission` | `premium_revenue * commission_rate` | Estimated agent earnings |

---

## Risk bands

Used in `gold_fraud_risk_summary` and `gold_claim_fraud_features`:

| Band | Condition | Meaning |
|---|---|---|
| `low` | `risk_score < 30` | Low fraud risk |
| `medium` | `30 <= risk_score < 70` | Moderate fraud risk, monitor |
| `high` | `risk_score >= 70` | High fraud risk, investigate |
