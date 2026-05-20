from pyspark.sql import functions as F

def add_bronze_metadata(df, ingest_run_id: str):
    return (
        df.withColumn("ingest_timestamp", F.current_timestamp())
          .withColumn("ingest_run_id", F.lit(ingest_run_id))
          .withColumn("source_file_name", F.input_file_name())
    )

def read_csv_raw(spark, path: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def write_delta_table(df, table_name: str, mode: str = "overwrite"):
    df.write.format("delta").mode(mode).saveAsTable(table_name)
