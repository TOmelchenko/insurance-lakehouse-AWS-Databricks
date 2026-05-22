# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard Queries
# MAGIC Preview each dashboard view.

# COMMAND ----------

catalog = "insurance_lakehouse"
gold_schema = "gold"

views = [
    "vw_executive_insurance_overview",
    "vw_claims_operations",
    "vw_policy_portfolio",
    "vw_fraud_risk_monitoring",
    "vw_agent_regional_performance",
    "vw_data_quality_monitoring"
]

for view in views:
    print(f"\nPreviewing {view}")
    display(spark.table(f"{catalog}.{gold_schema}.{view}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive overview - key KPIs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     total_policies,
# MAGIC     total_active_policies,
# MAGIC     total_cancelled_policies,
# MAGIC     total_premium_revenue,
# MAGIC     avg_premium,
# MAGIC     total_coverage
# MAGIC FROM insurance_lakehouse.gold.vw_executive_insurance_overview

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims operations - status breakdown

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     claim_status,
# MAGIC     SUM(total_claims)               AS total_claims,
# MAGIC     ROUND(SUM(total_claim_amount), 2) AS total_claim_amount,
# MAGIC     ROUND(AVG(avg_claim_amount), 2) AS avg_claim_amount,
# MAGIC     ROUND(AVG(fraud_flag_rate), 4)  AS fraud_flag_rate
# MAGIC FROM insurance_lakehouse.gold.vw_claims_operations
# MAGIC GROUP BY claim_status
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims operations - by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     bundesland,
# MAGIC     SUM(total_claims)               AS total_claims,
# MAGIC     ROUND(SUM(total_claim_amount), 2) AS total_claim_amount,
# MAGIC     ROUND(AVG(avg_risk_score), 2)   AS avg_risk_score
# MAGIC FROM insurance_lakehouse.gold.vw_claims_operations
# MAGIC GROUP BY bundesland
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy portfolio - premium by product type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     policy_type,
# MAGIC     SUM(total_policies)             AS total_policies,
# MAGIC     SUM(active_policies)            AS active_policies,
# MAGIC     ROUND(SUM(premium_revenue), 2)  AS premium_revenue,
# MAGIC     ROUND(AVG(avg_premium), 2)      AS avg_premium
# MAGIC FROM insurance_lakehouse.gold.vw_policy_portfolio
# MAGIC GROUP BY policy_type
# MAGIC ORDER BY premium_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy portfolio - by sales channel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sales_channel,
# MAGIC     SUM(total_policies)             AS total_policies,
# MAGIC     ROUND(SUM(premium_revenue), 2)  AS premium_revenue
# MAGIC FROM insurance_lakehouse.gold.vw_policy_portfolio
# MAGIC GROUP BY sales_channel
# MAGIC ORDER BY premium_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud risk - by risk band

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     risk_band,
# MAGIC     SUM(total_claims)               AS total_claims,
# MAGIC     SUM(high_risk_claims)           AS high_risk_claims,
# MAGIC     ROUND(AVG(fraud_risk_rate), 4)  AS fraud_risk_rate,
# MAGIC     ROUND(AVG(avg_risk_score), 2)   AS avg_risk_score,
# MAGIC     SUM(suspicious_amount_count)    AS suspicious_amount_count,
# MAGIC     SUM(duplicate_claim_count)      AS duplicate_claim_count
# MAGIC FROM insurance_lakehouse.gold.vw_fraud_risk_monitoring
# MAGIC GROUP BY risk_band
# MAGIC ORDER BY CASE risk_band WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud risk - by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     bundesland,
# MAGIC     SUM(total_claims)               AS total_claims,
# MAGIC     SUM(high_risk_claims)           AS high_risk_claims,
# MAGIC     ROUND(AVG(avg_risk_score), 2)   AS avg_risk_score
# MAGIC FROM insurance_lakehouse.gold.vw_fraud_risk_monitoring
# MAGIC GROUP BY bundesland
# MAGIC ORDER BY high_risk_claims DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent performance - top 10 by premium revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     agent_name,
# MAGIC     region,
# MAGIC     bundesland,
# MAGIC     total_policies_sold,
# MAGIC     premium_revenue,
# MAGIC     total_claims_linked,
# MAGIC     claims_ratio,
# MAGIC     estimated_commission
# MAGIC FROM insurance_lakehouse.gold.vw_agent_regional_performance
# MAGIC ORDER BY premium_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent performance - by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region,
# MAGIC     COUNT(agent_id)                         AS total_agents,
# MAGIC     SUM(total_policies_sold)                AS total_policies_sold,
# MAGIC     ROUND(SUM(premium_revenue), 2)          AS premium_revenue,
# MAGIC     ROUND(AVG(claims_ratio), 4)             AS avg_claims_ratio,
# MAGIC     ROUND(SUM(estimated_commission), 2)     AS total_commission
# MAGIC FROM insurance_lakehouse.gold.vw_agent_regional_performance
# MAGIC GROUP BY region
# MAGIC ORDER BY premium_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality - quarantine summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     error_reason,
# MAGIC     error_severity,
# MAGIC     quarantine_count,
# MAGIC     first_seen,
# MAGIC     last_seen
# MAGIC FROM insurance_lakehouse.gold.vw_data_quality_monitoring
# MAGIC ORDER BY source_table, quarantine_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality - total quarantine by dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     SUM(quarantine_count)   AS total_quarantined,
# MAGIC     COUNT(error_reason)     AS distinct_error_types
# MAGIC FROM insurance_lakehouse.gold.vw_data_quality_monitoring
# MAGIC GROUP BY source_table
# MAGIC ORDER BY total_quarantined DESC
