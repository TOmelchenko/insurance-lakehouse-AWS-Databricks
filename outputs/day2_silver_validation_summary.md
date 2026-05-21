# Day 2 Silver Validation Summary

Fill this after running validation notebook.
|dataset|bronze_count|silver_count|quarantine_count|unaccounted_drop|status|
|---|---|---|---|---|---|
|customers|10000|10000|0|0|PASS|
|policies|25000|25000|0|0|PASS|
|claims|50000|50000|0|0|PASS|
|payments|50000|50000|0|0|PASS|
|agents|1000|1000|0|0|PASS|
|fraud_indicators|50000|31715|0|18285|REVIEW|
18285 records with duplicated claim_id are under review. We keep the record with the highest risk_score per claim_id.

|dataset|has_ingest_timestamp|has_ingest_run_id|has_source_file_name|
|---|---|---|---|
|customers|true|true|true|
|policies|true|true|true|
|claims|true|true|true|
|payments|true|true|true|
|agents|true|true|true|
|fraud_indicators|true|true|true|