SELECT 'silver_customers' AS table_name, COUNT(*) AS row_count FROM insurance_lakehouse.silver.silver_customers
UNION ALL
SELECT 'silver_policies', COUNT(*) FROM insurance_lakehouse.silver.silver_policies
UNION ALL
SELECT 'silver_claims', COUNT(*) FROM insurance_lakehouse.silver.silver_claims
UNION ALL
SELECT 'silver_payments', COUNT(*) FROM insurance_lakehouse.silver.silver_payments
UNION ALL
SELECT 'silver_agents', COUNT(*) FROM insurance_lakehouse.silver.silver_agents
UNION ALL
SELECT 'silver_fraud_indicators', COUNT(*) FROM insurance_lakehouse.silver.silver_fraud_indicators;
