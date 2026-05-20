# S3 Folder Design

```text
s3://insurance-lakehouse-project-tomelchenko/
  raw/
    customers/
    policies/
    claims/
    payments/
    agents/
    fraud_indicators/
  bronze/
  silver/
  gold/
  checkpoints/
  quarantine/
```

`raw/` stores generated source-like data.  
`checkpoints/` is for Auto Loader / streaming checkpoints.  
`quarantine/` will be used on Day 2.