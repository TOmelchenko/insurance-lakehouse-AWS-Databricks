# Databricks notebook source

# MAGIC %md
# MAGIC # 24 — Dashboard Views
# MAGIC Create final dashboard-ready SQL views. Use Gold tables as inputs and avoid raw PII.

# COMMAND ----------

catalog = "insurance_lakehouse"
gold_schema = "gold"
bronze_schema = "bronze"
quarantine_schema = "quarantine"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# view 1 - executive overview
spark.sql(f'''
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_executive_insurance_overview AS
SELECT
    SUM(total_policies)                     AS total_policies,
    SUM(active_policies)                    AS total_active_policies,
    SUM(cancelled_policies)                 AS total_cancelled_policies,
    ROUND(SUM(premium_revenue), 2)          AS total_premium_revenue,
    ROUND(AVG(avg_premium), 2)              AS avg_premium,
    ROUND(SUM(total_coverage), 2)           AS total_coverage
FROM {catalog}.{gold_schema}.gold_policy_performance
''')
print("Created vw_executive_insurance_overview")

# COMMAND ----------

# view 2 - claims operations
spark.sql(f'''
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_claims_operations AS
SELECT
    claim_month,
    claim_status,
    claim_type,
    policy_type,
    bundesland,
    SUM(total_claims)                       AS total_claims,
    SUM(open_claims)                        AS open_claims,
    SUM(approved_claims)                    AS approved_claims,
    SUM(rejected_claims)                    AS rejected_claims,
    SUM(paid_claims)                        AS paid_claims,
    ROUND(SUM(total_claim_amount), 2)       AS total_claim_amount,
    ROUND(AVG(avg_claim_amount), 2)         AS avg_claim_amount,
    ROUND(AVG(avg_risk_score), 2)           AS avg_risk_score,
    ROUND(AVG(fraud_flag_rate), 4)          AS fraud_flag_rate
FROM {catalog}.{gold_schema}.gold_claims_overview
GROUP BY claim_month, claim_status, claim_type, policy_type, bundesland
''')
print("Created vw_claims_operations")

# COMMAND ----------

# view 3 - policy portfolio
spark.sql(f'''
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_policy_portfolio AS
SELECT
    policy_type,
    policy_status,
    sales_channel,
    bundesland,
    SUM(total_policies)                     AS total_policies,
    SUM(active_policies)                    AS active_policies,
    SUM(cancelled_policies)                 AS cancelled_policies,
    ROUND(SUM(premium_revenue), 2)          AS premium_revenue,
    ROUND(AVG(avg_premium), 2)              AS avg_premium,
    ROUND(SUM(total_coverage), 2)           AS total_coverage
FROM {catalog}.{gold_schema}.gold_policy_performance
GROUP BY policy_type, policy_status, sales_channel, bundesland
''')
print("Created vw_policy_portfolio")

# COMMAND ----------

# view 4 - fraud risk monitoring
spark.sql(f'''
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_fraud_risk_monitoring AS
SELECT
    bundesland,
    policy_type,
    claim_type,
    risk_band,
    SUM(total_claims)                       AS total_claims,
    SUM(high_risk_claims)                   AS high_risk_claims,
    ROUND(AVG(fraud_risk_rate), 4)          AS fraud_risk_rate,
    ROUND(AVG(avg_risk_score), 2)           AS avg_risk_score,
    SUM(suspicious_amount_count)            AS suspicious_amount_count,
    SUM(duplicate_claim_count)              AS duplicate_claim_count,
    SUM(late_report_count)                  AS late_report_count
FROM {catalog}.{gold_schema}.gold_fraud_risk_summary
GROUP BY bundesland, policy_type, claim_type, risk_band
''')
print("Created vw_fraud_risk_monitoring")

# COMMAND ----------

# view 5 - agent regional performance
spark.sql(f'''
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_agent_regional_performance AS
SELECT
    agent_id,
    agent_name,
    region,
    bundesland,
    active_flag,
    total_policies_sold,
    active_policies,
    ROUND(premium_revenue, 2)               AS premium_revenue,
    total_claims_linked,
    ROUND(total_claim_amount, 2)            AS total_claim_amount,
    ROUND(total_paid_amount, 2)             AS total_paid_amount,
    ROUND(claims_ratio, 4)                  AS claims_ratio,
    ROUND(estimated_commission, 2)          AS estimated_commission
FROM {catalog}.{gold_schema}.gold_agent_performance
''')
print("Created vw_agent_regional_performance")

# COMMAND ----------

# view 6 - data quality monitoring
spark.sql(f'''
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_data_quality_monitoring AS
SELECT
    source_table,
    error_reason,
    error_severity,
    COUNT(*)                                AS quarantine_count,
    MIN(quarantine_timestamp)               AS first_seen,
    MAX(quarantine_timestamp)               AS last_seen
FROM (
    SELECT source_table, error_reason, error_severity, quarantine_timestamp
    FROM {catalog}.{quarantine_schema}.quarantine_invalid_customers
    UNION ALL
    SELECT source_table, error_reason, error_severity, quarantine_timestamp
    FROM {catalog}.{quarantine_schema}.quarantine_invalid_policies
    UNION ALL
    SELECT source_table, error_reason, error_severity, quarantine_timestamp
    FROM {catalog}.{quarantine_schema}.quarantine_invalid_claims
    UNION ALL
    SELECT source_table, error_reason, error_severity, quarantine_timestamp
    FROM {catalog}.{quarantine_schema}.quarantine_invalid_payments
)
GROUP BY source_table, error_reason, error_severity
ORDER BY quarantine_count DESC
''')
print("Created vw_data_quality_monitoring")

# COMMAND ----------

print("\nAll dashboard views created:")
for view in [
    "vw_executive_insurance_overview",
    "vw_claims_operations",
    "vw_policy_portfolio",
    "vw_fraud_risk_monitoring",
    "vw_agent_regional_performance",
    "vw_data_quality_monitoring"
]:
    count = spark.table(f"{catalog}.{gold_schema}.{view}").count()
    print(f"  {view} - {count} rows")

