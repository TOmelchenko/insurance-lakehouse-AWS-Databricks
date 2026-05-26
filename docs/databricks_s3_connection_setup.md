# Databricks to AWS S3 - Connection Setup

This guide explains how to connect Databricks to an AWS S3 bucket so you can read and write files from notebooks. It covers the recommended Serverless approach using Unity Catalog external locations.

---

## Why this matters

On Databricks Serverless compute, the old method of passing AWS credentials in code does not work:

```python
# This does NOT work on Serverless
spark.conf.set("fs.s3a.access.key", "AKIA...")
spark.conf.set("fs.s3a.secret.key", "...")
```

Serverless compute is locked down for security and authenticates through Unity Catalog instead. The correct pattern is:

**S3 bucket → IAM role → Unity Catalog external location → Serverless compute**

Once set up, your notebooks can read and write S3 paths with zero credentials in code:

```python
df.write.csv("s3://your-bucket/data/")  # just works
```

---

## Prerequisites

- Databricks workspace on AWS (created via AWS Marketplace)
- AWS account with permissions to create IAM roles and CloudFormation stacks
- An S3 bucket in the same AWS region as your Databricks workspace
- Unity Catalog metastore assigned to your workspace

---

## Step 1 - Verify Unity Catalog is enabled

1. Go to the Databricks account console: `https://accounts.cloud.databricks.com`
2. Click **Catalog** in the left sidebar
3. Confirm a metastore exists for your region
4. If not, click **Create metastore** and set the region to match your workspace (e.g. `us-east-2`)
5. Go to **Workspaces**, click your workspace, and assign the metastore

If your workspace already has Catalog Explorer visible in the left sidebar, this is done.

---

## Step 2 - Confirm region alignment

This is the most common source of setup failures. All three must be in the same AWS region:

- Databricks workspace
- S3 bucket
- CloudFormation stack (will create in Step 4)

To check the Databricks region: look at your workspace URL.
- `dbc-xxxxx.cloud.databricks.com` - check the workspace settings page for region
- The region appears in the workspace URL on the account console

If your S3 bucket is in `us-east-1` but your workspace is in `us-east-2`, create a new bucket in the workspace region. Cross-region access works but adds latency and cost.

---

## Step 3 - Start the external location Quickstart

1. Open your Databricks workspace
2. Click **Catalog** in the left sidebar
3. Click the **gear icon** at the top of the Catalog panel
4. Click **External Locations**
5. Click **Create external location**
6. Choose **Quickstart** (NOT "Manual")

A dialog opens asking for:

- **Bucket name** - enter your S3 bucket name (just the name, no `s3://` prefix)
- **Personal Access Token** - Databricks generates one automatically for the CloudFormation stack

**Copy the token** - you will paste it into AWS Console in the next step.

Then click the button to proceed to AWS Console.

---

## Step 4 - Create the CloudFormation stack

A new browser tab opens with AWS Console at the **Create stack** page. The form is pre-filled by Databricks.

1. Confirm you are in the **correct AWS region** (top right corner of AWS Console)
2. Paste your Personal Access Token into the **Databricks Personal Access Token** field
3. Verify the bucket name and workspace URL are correct
4. Acknowledge the IAM resource creation checkbox at the bottom
5. Click **Create stack**

Wait 1-2 minutes for the stack to reach **CREATE_COMPLETE** status.

### What CloudFormation creates

The stack creates everything needed in one shot:

| Resource | Purpose |
|---|---|
| IAM role | Grants Databricks permission to access your bucket |
| Trust policy | Allows Databricks AWS account to assume the role |
| S3 bucket policy | Grants the IAM role read/write/list/delete on the bucket |
| Instance profile | Links the IAM role to Databricks compute |
| External location | Auto-registers the bucket in Unity Catalog |

If you check AWS Console after the stack completes:
- **IAM > Roles** - look for `databricks-s3-ingest-xxxxx-db_s3_iam`
- **S3 > your bucket > Permissions > Bucket policy** - shows the policy added
- **CloudFormation > Stacks** - shows the stack and its events

---

## Step 5 - Validate the connection

1. Return to your Databricks workspace
2. Catalog > gear icon > **External Locations**
3. Click your newly created external location
4. Click **Test connection**

You should see green checks for all of these:

| Check | What it verifies |
|---|---|
| Read | Can list and read files from the bucket |
| Write | Can create files in the bucket |
| Delete | Can delete files in the bucket |
| Assume Role | The IAM role can be assumed by Databricks |
| Self Assume Role | Role can assume itself (required for some operations) |
| External ID Condition | Trust policy is correctly scoped |

If any check fails, see the troubleshooting section below.

---

## Step 6 - Verify from a notebook

Open a notebook on **Serverless compute** and run:

```python
# test write
df = spark.createDataFrame([("hello",)], ["value"])
df.write.mode("overwrite").csv("s3://your-bucket-name/test/")
print("write done")

# test read
dbutils.fs.ls("s3://your-bucket-name/")
```

If both work, the connection is fully set up. You can now use S3 paths anywhere in your notebooks without configuring credentials.

---

## How to use the connection

Once set up, S3 access works automatically with no extra setup:

### Reading CSV files

```python
df = spark.read.option("header", True).csv("s3://your-bucket/raw/")
```

### Writing Delta tables to S3-backed paths

```python
df.write.format("delta").mode("overwrite").save("s3://your-bucket/silver/customers/")
```

### Autoloader incremental ingestion

```python
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "s3://your-bucket/checkpoints/customers/")
    .option("header", True)
    .load("s3://your-bucket/raw/customers/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "s3://your-bucket/checkpoints/customers/")
    .trigger(availableNow=True)
    .toTable("your_catalog.bronze.bronze_customers")
    .awaitTermination()
)
```

The `s3://` paths just work - Serverless looks up the matching external location and uses the IAM role automatically.

---

## Why this works

Serverless compute does not have direct internet or AWS credentials. Instead:

1. Your notebook tries to write to `s3://your-bucket/raw/`
2. Databricks looks for a registered external location matching that S3 path
3. The external location points to an IAM role
4. Databricks Serverless assumes that role
5. The role has bucket policy permissions to read/write your bucket
6. The write succeeds

No credentials in your code. No secrets in Spark config. No environment variables. Authentication is fully handled by Unity Catalog.

---

## Troubleshooting

### "Permission denied" on S3 access

Most common cause is **region mismatch**. Check that the CloudFormation stack, S3 bucket, and Databricks workspace are all in the same AWS region. Switch the AWS Console region (top right) to verify the stack exists in the expected region.

### CloudFormation stack not visible

The stack is created in whichever region was selected in AWS Console when you clicked **Create stack**. If you can't find it:
1. Note your Databricks workspace region
2. Switch to that region in AWS Console (top right dropdown)
3. Go to CloudFormation > Stacks

### "External location not found" error in notebook

The external location must include the exact S3 path you are accessing. If your external location covers `s3://bucket/raw/` but you try to write to `s3://bucket/data/`, it will fail. Either:
- Create the external location at the bucket root: `s3://bucket/`
- Or create multiple external locations for different prefixes

### Direct credential method doesn't work

This is expected on Serverless. Direct credentials only work on classic clusters with the right configuration. Use external locations instead - they are the recommended pattern for all new workloads.

### "Cannot assume role" in Test Connection

The IAM trust policy may have an incorrect External ID. Delete the CloudFormation stack and re-run the Quickstart from Databricks - this regenerates the token and trust policy.

---

## Alternative methods (not recommended)

For completeness, these other methods exist but should be avoided for new projects:

| Method | Why avoid |
|---|---|
| Instance profiles on classic clusters | Doesn't work on Serverless; legacy approach |
| `spark.conf.set` with access keys | Doesn't work on Serverless; credentials in code |
| `dbutils.secrets.get` with hardcoded keys | Doesn't work on Serverless; manual key rotation |
| Mounting S3 to DBFS | Deprecated in Unity Catalog; doesn't work on Serverless |

Unity Catalog external locations are the only forward-compatible approach.

---

## Summary checklist

| Step | Status |
|---|---|
| Unity Catalog metastore assigned to workspace | ☐ |
| S3 bucket exists in same region as workspace | ☐ |
| CloudFormation stack created in correct region | ☐ |
| External location registered in Unity Catalog | ☐ |
| Test Connection shows all green | ☐ |
| Notebook write to `s3://` path succeeds | ☐ |
| Notebook read from `s3://` path succeeds | ☐ |

Once all 7 checks pass, your Databricks workspace can read and write any S3 path on Serverless compute with no credentials in code.
