SELECT COUNT(*) AS claims_without_policy
FROM insurance_lakehouse.silver.silver_claims c
LEFT ANTI JOIN insurance_lakehouse.silver.silver_policies p
ON c.policy_id = p.policy_id;

SELECT COUNT(*) AS payments_without_claim
FROM insurance_lakehouse.silver.silver_payments p
LEFT ANTI JOIN insurance_lakehouse.silver.silver_claims c
ON p.claim_id = c.claim_id;
