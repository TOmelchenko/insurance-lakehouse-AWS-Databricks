# Databricks notebook source
# MAGIC %md
# MAGIC # Governance and GDPR Final Design
# MAGIC Inspect final dashboard views and check whether raw PII is exposed.

# COMMAND ----------

pii_fields = ["first_name", "last_name", "email", "phone_number", "street", "postal_code", "date_of_birth", "iban", "iban_hash"]
catalog = "insurance_lakehouse"
gold_schema = "gold"
dashboard_views = [
    "vw_executive_insurance_overview",
    "vw_claims_operations",
    "vw_policy_portfolio",
    "vw_fraud_risk_monitoring",
    "vw_agent_regional_performance",
    "vw_data_quality_monitoring"
]

for view in dashboard_views:
    df = spark.table(f"{catalog}.{gold_schema}.{view}")
    exposed = [p for p in pii_fields if p.lower() in [c.lower() for c in df.columns]]
    print(f"{view}: exposed PII fields = {exposed}")
