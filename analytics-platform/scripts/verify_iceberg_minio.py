from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_HADOOP_HOME = PROJECT_ROOT / "tools" / "hadoop"

os.environ.setdefault("HADOOP_HOME", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("hadoop.home.dir", str(LOCAL_HADOOP_HOME))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession


ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
WAREHOUSE_PATH = "s3a://warehouse/iceberg"


def create_spark() -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE])
    return (
        SparkSession.builder.appName("verify-iceberg-minio")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE_PATH)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .getOrCreate()
    )


def main() -> None:
    print("Starting Spark session...", flush=True)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Creating Iceberg namespace...", flush=True)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.sales_ods")
        print("Dropping smoke test table if it exists...", flush=True)
        spark.sql("DROP TABLE IF EXISTS lake.sales_ods.ods_iceberg_smoke_test")
        print("Creating smoke test table...", flush=True)
        spark.sql(
            """
            CREATE TABLE lake.sales_ods.ods_iceberg_smoke_test (
                id BIGINT,
                user_name STRING,
                amount DOUBLE
            ) USING iceberg
            """
        )
        print("Inserting smoke test rows...", flush=True)
        spark.sql(
            """
            INSERT INTO lake.sales_ods.ods_iceberg_smoke_test VALUES
            (1, 'alpha', 10.5),
            (2, 'beta', 20.0),
            (3, 'gamma', 30.5)
            """
        )
        print("Reading smoke test table...", flush=True)
        result = spark.sql(
            """
            SELECT
                COUNT(*) AS row_count,
                ROUND(SUM(amount), 2) AS amount_sum
            FROM lake.sales_ods.ods_iceberg_smoke_test
            """
        ).first()

        print("Iceberg MinIO verification completed.")
        print(f"table=lake.sales_ods.ods_iceberg_smoke_test row_count={result['row_count']} amount_sum={result['amount_sum']}")
        print(f"warehouse={WAREHOUSE_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
