# S3 Folder Design

```text
s3://insurance-lakehouse-project/
  raw/
    customers/
    policies/
    claims/
    payments/
    agents/
    fraud_indicators/
  checkpoints/
```

`raw/` stores generated source-like data.  
`checkpoints/` is for Auto Loader / streaming checkpoints.  
