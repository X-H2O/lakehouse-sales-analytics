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
from pyspark.sql import functions as F


ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
WAREHOUSE_PATH = "s3a://warehouse/iceberg"


def create_spark() -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE])
    return (
        SparkSession.builder.appName("build-dwd-tables")
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


def table(spark: SparkSession, name: str) -> DataFrame:
    return spark.table(f"lake.sales_ods.ods_{name}")


def replace_table(df: DataFrame, full_table_name: str) -> int:
    spark = df.sparkSession
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    df.writeTo(full_table_name).using("iceberg").create()
    return spark.table(full_table_name).count()


def build_order_detail(spark: SparkSession) -> DataFrame:
    orders = table(spark, "orders").alias("o")
    order_items = table(spark, "order_items").alias("oi")
    users = table(spark, "users").alias("u")
    products = table(spark, "products").alias("p")
    channels = table(spark, "channels").alias("c")

    return (
        order_items.join(orders, F.col("oi.order_id") == F.col("o.order_id"), "inner")
        .join(users, F.col("o.user_id") == F.col("u.user_id"), "left")
        .join(products, F.col("oi.product_id") == F.col("p.product_id"), "left")
        .join(channels, F.col("o.channel_id") == F.col("c.channel_id"), "left")
        .select(
            F.col("oi.order_item_id"),
            F.col("o.order_id"),
            F.col("o.user_id"),
            F.col("u.user_name"),
            F.col("u.gender"),
            F.col("u.province"),
            F.col("u.city"),
            F.col("o.channel_id"),
            F.col("c.channel_name"),
            F.col("c.channel_type"),
            F.col("oi.product_id"),
            F.col("p.sku"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.brand"),
            F.col("oi.quantity"),
            F.col("oi.unit_price"),
            F.col("oi.item_amount"),
            F.col("p.cost_price"),
            F.round(F.col("oi.item_amount") - F.col("p.cost_price") * F.col("oi.quantity"), 2).alias("gross_profit"),
            F.col("o.total_amount").alias("order_total_amount"),
            F.col("o.payment_method"),
            F.col("o.status").alias("order_status"),
            F.when(F.col("o.status") == "paid", F.lit(1)).otherwise(F.lit(0)).alias("is_paid"),
            F.col("o.order_time"),
            F.to_date("o.order_time").alias("order_date"),
            F.year("o.order_time").alias("order_year"),
            F.month("o.order_time").alias("order_month"),
            F.current_timestamp().alias("dwd_updated_at"),
        )
    )


def build_visit_detail(spark: SparkSession) -> DataFrame:
    visits = table(spark, "visits").alias("v")
    users = table(spark, "users").alias("u")
    products = table(spark, "products").alias("p")
    channels = table(spark, "channels").alias("c")

    return (
        visits.join(users, F.col("v.user_id") == F.col("u.user_id"), "left")
        .join(products, F.col("v.product_id") == F.col("p.product_id"), "left")
        .join(channels, F.col("v.channel_id") == F.col("c.channel_id"), "left")
        .select(
            F.col("v.visit_id"),
            F.col("v.session_id"),
            F.col("v.user_id"),
            F.col("u.user_name"),
            F.col("u.gender"),
            F.col("u.province"),
            F.col("u.city"),
            F.col("v.channel_id"),
            F.col("c.channel_name"),
            F.col("c.channel_type"),
            F.col("v.product_id"),
            F.col("p.sku"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.brand"),
            F.col("v.event_type"),
            F.col("v.visit_time"),
            F.to_date("v.visit_time").alias("visit_date"),
            F.year("v.visit_time").alias("visit_year"),
            F.month("v.visit_time").alias("visit_month"),
            F.current_timestamp().alias("dwd_updated_at"),
        )
    )


def build_refund_detail(spark: SparkSession) -> DataFrame:
    refunds = table(spark, "refunds").alias("r")
    orders = table(spark, "orders").alias("o")
    order_items = table(spark, "order_items").alias("oi")
    users = table(spark, "users").alias("u")
    products = table(spark, "products").alias("p")
    channels = table(spark, "channels").alias("c")

    return (
        refunds.join(orders, F.col("r.order_id") == F.col("o.order_id"), "left")
        .join(order_items, F.col("r.order_item_id") == F.col("oi.order_item_id"), "left")
        .join(users, F.col("r.user_id") == F.col("u.user_id"), "left")
        .join(products, F.col("r.product_id") == F.col("p.product_id"), "left")
        .join(channels, F.col("o.channel_id") == F.col("c.channel_id"), "left")
        .select(
            F.col("r.refund_id"),
            F.col("r.order_id"),
            F.col("r.order_item_id"),
            F.col("r.user_id"),
            F.col("u.user_name"),
            F.col("u.province"),
            F.col("u.city"),
            F.col("o.channel_id"),
            F.col("c.channel_name"),
            F.col("c.channel_type"),
            F.col("r.product_id"),
            F.col("p.sku"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.brand"),
            F.col("oi.quantity"),
            F.col("oi.item_amount"),
            F.col("r.refund_amount"),
            F.col("r.reason").alias("refund_reason"),
            F.col("r.status").alias("refund_status"),
            F.when(F.col("r.status") == "approved", F.lit(1)).otherwise(F.lit(0)).alias("is_approved"),
            F.col("o.order_time"),
            F.to_date("o.order_time").alias("order_date"),
            F.col("r.refund_time"),
            F.to_date("r.refund_time").alias("refund_date"),
            F.year("r.refund_time").alias("refund_year"),
            F.month("r.refund_time").alias("refund_month"),
            F.current_timestamp().alias("dwd_updated_at"),
        )
    )


def main() -> None:
    print("Starting Iceberg DWD table build.", flush=True)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.sales_dwd")

        jobs = [
            ("dwd_order_detail", build_order_detail(spark), table(spark, "order_items").count()),
            ("dwd_visit_detail", build_visit_detail(spark), table(spark, "visits").count()),
            ("dwd_refund_detail", build_refund_detail(spark), table(spark, "refunds").count()),
        ]

        print("\nBuilt DWD tables")
        for table_name, df, expected_count in jobs:
            actual_count = replace_table(df, f"lake.sales_dwd.{table_name}")
            status = "OK" if actual_count == expected_count else "MISMATCH"
            print(f"  {table_name:<20} rows={actual_count:>8} expected={expected_count:>8} {status}", flush=True)

        print("\nIceberg DWD table build completed.")
        print(f"warehouse={WAREHOUSE_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
