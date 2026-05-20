# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # 01 — Generate Synthetic German Insurance Data
# MAGIC Generate customers, policies, claims, payments, agents, and fraud indicators.

# COMMAND ----------

from pyspark.sql import functions as F

DATA_MODE = "small"

sizes = {
    "small": {"customers": 10000, "policies": 25000, "claims": 50000, "payments": 50000, "agents": 1000, "fraud_indicators": 50000, "partitions": 8},
    "medium": {"customers": 500000, "policies": 1500000, "claims": 5000000, "payments": 5000000, "agents": 10000, "fraud_indicators": 5000000, "partitions": 128},
    "large": {"customers": 2000000, "policies": 5000000, "claims": 20000000, "payments": 20000000, "agents": 25000, "fraud_indicators": 20000000, "partitions": 512}
}
cfg = sizes[DATA_MODE]

german_cities = ["Berlin","Hamburg","München","Köln","Frankfurt","Stuttgart","Düsseldorf","Dortmund","Essen","Leipzig","Bremen","Hannover","Nürnberg","Dresden"]
bundeslaender = ["Berlin","Hamburg","Bayern","Nordrhein-Westfalen","Hessen","Baden-Württemberg","Sachsen","Niedersachsen","Bremen"]

def array_expr(values):
    return "array(" + ",".join([f"'{v}'" for v in values]) + ")"

def pick(values):
    return F.expr(f"element_at({array_expr(values)}, cast(rand()*{len(values)}+1 as int))")

# COMMAND ----------

customers_df = (
    spark.range(cfg["customers"])
    .withColumn("customer_id", F.concat(F.lit("CUST_"), F.lpad(F.col("id").cast("string"), 10, "0")))
    .withColumn("first_name", F.concat(F.lit("FirstName_"), F.col("id")))
    .withColumn("last_name", F.concat(F.lit("LastName_"), F.col("id")))
    .withColumn("date_of_birth", F.date_sub(F.current_date(), (F.rand()*25000+7000).cast("int")))
    .withColumn("gender", pick(["male","female","diverse","unknown"]))
    .withColumn("email", F.concat(F.lit("customer_"), F.col("id"), F.lit("@example-insurance.de")))
    .withColumn("phone_number", F.concat(F.lit("+49"), (F.rand()*10000000000).cast("bigint").cast("string")))
    .withColumn("street", F.concat(F.lit("Musterstraße "), (F.rand()*250).cast("int").cast("string")))
    .withColumn("city", pick(german_cities))
    .withColumn("postal_code", F.lpad((F.rand()*99999).cast("int").cast("string"), 5, "0"))
    .withColumn("bundesland", pick(bundeslaender))
    .withColumn("country", F.lit("Germany"))
    .withColumn("registration_date", F.date_sub(F.current_date(), (F.rand()*3650).cast("int")))
    .withColumn("gdpr_consent", F.rand() > 0.1)
    .withColumn("customer_segment", pick(["retail","premium","corporate","senior","student"]))
    .drop("id")
)

agents_df = (
    spark.range(cfg["agents"])
    .withColumn("agent_id", F.concat(F.lit("AGENT_"), F.lpad(F.col("id").cast("string"), 8, "0")))
    .withColumn("agent_name", F.concat(F.lit("Agentur Rheinland "), F.col("id")))
    .withColumn("region", pick(["North","South","East","West","Central"]))
    .withColumn("city", pick(german_cities))
    .withColumn("bundesland", pick(bundeslaender))
    .withColumn("commission_rate", F.round(F.rand()*0.12+0.02, 4))
    .withColumn("active_flag", F.rand() > 0.05)
    .drop("id")
)

policies_df = (
    spark.range(cfg["policies"])
    .withColumn("policy_id", F.concat(F.lit("POL_"), F.lpad(F.col("id").cast("string"), 12, "0")))
    .withColumn("customer_num", (F.rand()*cfg["customers"]).cast("long"))
    .withColumn("customer_id", F.concat(F.lit("CUST_"), F.lpad(F.col("customer_num").cast("string"), 10, "0")))
    .withColumn("policy_type", pick(["car","home","health","travel","liability"]))
    .withColumn("start_date", F.date_sub(F.current_date(), (F.rand()*2500).cast("int")))
    .withColumn("end_date", F.date_add(F.col("start_date"), (F.rand()*1500+365).cast("int")))
    .withColumn("premium_amount", F.round(F.rand()*2000+100, 2))
    .withColumn("coverage_amount", F.round(F.col("premium_amount")*(F.rand()*300+20), 2))
    .withColumn("policy_status", pick(["active","cancelled","expired"]))
    .withColumn("agent_num", (F.rand()*cfg["agents"]).cast("long"))
    .withColumn("agent_id", F.concat(F.lit("AGENT_"), F.lpad(F.col("agent_num").cast("string"), 8, "0")))
    .withColumn("sales_channel", pick(["online","broker","branch","partner"]))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .drop("id","customer_num","agent_num")
)

claims_df = (
    spark.range(cfg["claims"])
    .withColumn("claim_id", F.concat(F.lit("CLM_"), F.lpad(F.col("id").cast("string"), 12, "0")))
    .withColumn("policy_num", (F.rand()*cfg["policies"]).cast("long"))
    .withColumn("policy_id", F.concat(F.lit("POL_"), F.lpad(F.col("policy_num").cast("string"), 12, "0")))
    .withColumn("customer_num", (F.rand()*cfg["customers"]).cast("long"))
    .withColumn("customer_id", F.concat(F.lit("CUST_"), F.lpad(F.col("customer_num").cast("string"), 10, "0")))
    .withColumn("claim_date", F.date_sub(F.current_date(), (F.rand()*1000).cast("int")))
    .withColumn("claim_type", pick(["accident","theft","damage","health_cost","travel_cancel","liability"]))
    .withColumn("claim_amount", F.round(F.rand()*25000+50, 2))
    .withColumn("claim_status", pick(["open","approved","rejected","under_review","paid"]))
    .withColumn("claim_description", F.concat(F.lit("Synthetic claim description "), F.col("id")))
    .withColumn("reported_channel", pick(["app","phone","broker","email","branch"]))
    .withColumn("fraud_flag", F.rand() > 0.96)
    .withColumn("created_at", F.current_timestamp())
    .drop("id","policy_num","customer_num")
)

payments_df = (
    spark.range(cfg["payments"])
    .withColumn("payment_id", F.concat(F.lit("PAY_"), F.lpad(F.col("id").cast("string"), 12, "0")))
    .withColumn("claim_num", (F.rand()*cfg["claims"]).cast("long"))
    .withColumn("claim_id", F.concat(F.lit("CLM_"), F.lpad(F.col("claim_num").cast("string"), 12, "0")))
    .withColumn("payment_date", F.date_sub(F.current_date(), (F.rand()*900).cast("int")))
    .withColumn("payment_amount", F.round(F.rand()*20000, 2))
    .withColumn("payment_status", pick(["paid","pending","rejected"]))
    .withColumn("payment_method", pick(["SEPA","bank_transfer","card"]))
    .withColumn("iban_hash", F.sha2(F.concat(F.lit("DE"), F.col("id").cast("string"), F.lit("_synthetic_iban")), 256))
    .withColumn("created_at", F.current_timestamp())
    .drop("id","claim_num")
)

fraud_indicators_df = (
    spark.range(cfg["fraud_indicators"])
    .withColumn("claim_num", (F.rand()*cfg["claims"]).cast("long"))
    .withColumn("claim_id", F.concat(F.lit("CLM_"), F.lpad(F.col("claim_num").cast("string"), 12, "0")))
    .withColumn("previous_claims_count", (F.rand()*10).cast("int"))
    .withColumn("suspicious_amount_flag", F.rand() > 0.85)
    .withColumn("duplicate_claim_flag", F.rand() > 0.95)
    .withColumn("late_report_flag", F.rand() > 0.80)
    .withColumn("high_risk_region_flag", F.rand() > 0.88)
    .withColumn("risk_score", (F.rand()*100).cast("int"))
    .drop("id","claim_num")
)

for name, df in {
    "customers": customers_df, "policies": policies_df, "claims": claims_df,
    "payments": payments_df, "agents": agents_df, "fraud_indicators": fraud_indicators_df
}.items():
    df.createOrReplaceTempView(f"tmp_{name}")
    print(name, df.count())

display(customers_df.limit(10))

# COMMAND ----------

#S3_BUCKET = "s3://insurance-lakehouse-project-tomelchenko"
S3_BUCKET = "s3://databricks-sfjvkaqmnimrsa4tfyg8cs-cloud-storage-bucket"
RAW_BASE_PATH = f"{S3_BUCKET}/raw"
DATA_MODE = "small"
PARTITIONS = 8 if DATA_MODE == "small" else 128

datasets = {
    "customers": spark.table("tmp_customers"),
    "policies": spark.table("tmp_policies"),
    "claims": spark.table("tmp_claims"),
    "payments": spark.table("tmp_payments"),
    "agents": spark.table("tmp_agents"),
    "fraud_indicators": spark.table("tmp_fraud_indicators"),
}

for name, df in datasets.items():
    target_path = f"{RAW_BASE_PATH}/{name}"
    print("Writing", name, "to", target_path)
    df.repartition(PARTITIONS).write.mode("overwrite").option("header", True).csv(target_path)

raw_counts = []
for name in datasets:
    count_value = spark.read.option("header", True).csv(f"{RAW_BASE_PATH}/{name}").count()
    raw_counts.append((name, count_value))
display(spark.createDataFrame(raw_counts, ["dataset", "raw_count"]))
