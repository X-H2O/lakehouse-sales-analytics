from __future__ import annotations

import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("HADOOP_HOME", str(PROJECT_ROOT / "analytics-platform" / "tools" / "hadoop"))
os.environ.setdefault("hadoop.home.dir", str(PROJECT_ROOT / "analytics-platform" / "tools" / "hadoop"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"

MINIO_ENDPOINT = "http://localhost:9002"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
WAREHOUSE_PATH = "s3a://warehouse/iceberg"


def create_spark() -> SparkSession:
    packages = ",".join([ICEBERG_PACKAGE, HADOOP_AWS_PACKAGE])
    return (
        SparkSession.builder.appName("verify-lakehouse-features")
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
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    results = {}
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.demo")
        spark.sql("DROP TABLE IF EXISTS lake.demo.products_demo")

        # 1) Create table via writeTo (JVM-side range data, no Python worker)
        df1 = spark.range(1, 3).selectExpr("cast(id as int) id", "concat('P', id) name", "id * 10.0 price")
        df1.coalesce(1).writeTo("lake.demo.products_demo").using("iceberg").create()
        rows_v1 = spark.sql("SELECT * FROM lake.demo.products_demo").count()
        snap1 = spark.sql("SELECT snapshot_id FROM lake.demo.products_demo.snapshots ORDER BY committed_at ASC LIMIT 1").collect()[0][0]

        # 2) Append -> new snapshot
        df2 = spark.range(3, 4).selectExpr("cast(id as int) id", "concat('P', id) name", "id * 10.0 price")
        df2.coalesce(1).writeTo("lake.demo.products_demo").append()
        rows_v2 = spark.sql("SELECT * FROM lake.demo.products_demo").count()
        snap2 = spark.sql("SELECT snapshot_id FROM lake.demo.products_demo.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]

        # 3) Time travel to snapshot 1
        rows_tt = spark.sql(f"SELECT * FROM lake.demo.products_demo VERSION AS OF {snap1}").count()
        names_tt = [r[0] for r in spark.sql(f"SELECT name FROM lake.demo.products_demo VERSION AS OF {snap1} ORDER BY id").collect()]

        # 4) Schema evolution: ALTER TABLE ADD COLUMN (DDL)
        schema_evolved = False
        cols_after = []
        try:
            spark.sql("ALTER TABLE lake.demo.products_demo ADD COLUMN category STRING")
            spark.range(4, 5).selectExpr("cast(id as int) id", "concat('P', id) name", "id * 10.0 price", "'book' category").coalesce(1).writeTo("lake.demo.products_demo").append()
            cols_after = [r[0] for r in spark.sql("DESCRIBE lake.demo.products_demo").collect()]
            schema_evolved = "category" in cols_after
        except Exception as e:
            cols_after = [r[0] for r in spark.sql("DESCRIBE lake.demo.products_demo").collect()]
            schema_evolved = False
            results["schema_evolution_error"] = str(e)[:200]

        snapshot_list = [(r[0], str(r[1])) for r in spark.sql(
            "SELECT snapshot_id, committed_at FROM lake.demo.products_demo.snapshots ORDER BY committed_at"
        ).collect()]

        results.update({
            "table": "lake.demo.products_demo",
            "snapshot_count": len(snapshot_list),
            "snapshots": [{"snapshot_id": s, "committed_at": t} for s, t in snapshot_list],
            "snapshot_1_id": snap1,
            "snapshot_2_id": snap2,
            "rows_after_v1": rows_v1,
            "rows_after_v2": rows_v2,
            "time_travel_rows_at_snapshot_1": rows_tt,
            "time_travel_names_at_snapshot_1": names_tt,
            "time_travel_works": rows_tt == 2,
            "schema_evolution_added_column": "category",
            "columns_after_evolution": cols_after,
            "schema_evolution_works": schema_evolved,
        })
        print(json.dumps(results, indent=2, ensure_ascii=False))
    finally:
        spark.stop()

    out = PROJECT_ROOT / "benchmarks" / "results" / "lakehouse_features.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()