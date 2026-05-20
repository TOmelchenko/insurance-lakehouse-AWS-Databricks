def raw_csv_count(spark, path: str) -> int:
    return spark.read.option("header", True).csv(path).count()

def table_count(spark, table_name: str) -> int:
    return spark.table(table_name).count()

def compare_counts(dataset_name: str, raw_count: int, bronze_count: int) -> dict:
    return {
        "dataset": dataset_name,
        "raw_count": raw_count,
        "bronze_count": bronze_count,
        "status": "PASS" if raw_count == bronze_count else "FAIL"
    }
