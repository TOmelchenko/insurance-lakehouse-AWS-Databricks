# Lakehouse Architecture

```text
Synthetic insurance data generation in Databricks
        ↓
Raw files written to AWS S3
        ↓
Bronze Delta ingestion
        ↓
Silver cleaning and data quality validation
        ↓
Quarantine invalid records
        ↓
Gold insurance KPIs and fraud analytics
        ↓
Dashboard SQL views
        ↓
Final business presentation and portfolio documentation
```