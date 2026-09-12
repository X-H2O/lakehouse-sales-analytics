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

from pyspark.sql import DataFrame, SparkSession


ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"
POSTGRES_PACKAGE = "org.postgresql:postgresql:42.7.8"

POSTGRES_URL = "jdbc:postgresql://localhost:5432/sales_ods"
POSTGRES_PROPS = {
    "user": "sales_user",
    "password": "sales_password",
    "driver": "org.postgresql.Driver",
}

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
WAREHOUSE_PATH = "s3a://warehouse/iceberg"

SOURCE_TABLES = [
    "channels",
    "users",
    "products",
    "visits",
    "orders",
    "order_items",
    "refunds",
]


def create_spark() -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE, POSTGRES_PACKAGE])
    return (
        SparkSession.builder.appName("extract-ods-to-iceberg")
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


def read_source_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPS)


def replace_iceberg_table(spark: SparkSession, source_table: str) -> tuple[int, int]:
    target_table = f"ods_{source_table}"
    full_table_name = f"lake.sales_ods.{target_table}"

    df = read_source_table(spark, source_table)
    source_count = df.count()

    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    df.writeTo(full_table_name).using("iceberg").create()

    iceberg_count = spark.table(full_table_name).count()
    return source_count, iceberg_count


def main() -> None:
    print("Starting PostgreSQL to Iceberg ODS extraction.", flush=True)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.sales_ods")

        print("\nExtracted ODS tables")
        for source_table in SOURCE_TABLES:
            source_count, iceberg_count = replace_iceberg_table(spark, source_table)
            status = "OK" if source_count == iceberg_count else "MISMATCH"
            print(
                f"  {source_table:<12} -> ods_{source_table:<18} "
                f"source={source_count:>8} iceberg={iceberg_count:>8} {status}",
                flush=True,
            )

        print("\nPostgreSQL to Iceberg ODS extraction completed.")
        print(f"warehouse={WAREHOUSE_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
