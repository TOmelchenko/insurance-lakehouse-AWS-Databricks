SELECT 'quarantine_invalid_customers' AS table_name, COUNT(*) AS row_count
FROM insurance_lakehouse.quarantine.quarantine_invalid_customers
UNION ALL
SELECT 'quarantine_invalid_policies', COUNT(*)
FROM insurance_lakehouse.quarantine.quarantine_invalid_policies
UNION ALL
SELECT 'quarantine_invalid_claims', COUNT(*)
FROM insurance_lakehouse.quarantine.quarantine_invalid_claims
UNION ALL
SELECT 'quarantine_invalid_payments', COUNT(*)
FROM insurance_lakehouse.quarantine.quarantine_invalid_payments;
