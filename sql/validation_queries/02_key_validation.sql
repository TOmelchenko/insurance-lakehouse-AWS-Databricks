SELECT policy_id, COUNT(*) AS duplicate_count
FROM insurance_lakehouse.silver.silver_policies
GROUP BY policy_id
HAVING COUNT(*) > 1;

SELECT claim_id, COUNT(*) AS duplicate_count
FROM insurance_lakehouse.silver.silver_claims
GROUP BY claim_id
HAVING COUNT(*) > 1;

SELECT payment_id, COUNT(*) AS duplicate_count
FROM insurance_lakehouse.silver.silver_payments
GROUP BY payment_id
HAVING COUNT(*) > 1;
