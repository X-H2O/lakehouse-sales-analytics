from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from urllib.request import urlopen
from pathlib import Path

import clickhouse_connect
import psycopg2


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_JAVA_HOME = r"C:\studio\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sales_ods",
    "user": "sales_user",
    "password": "sales_password",
}

CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 8123,
    "database": "sales_ads",
    "username": "sales_user",
    "password": "sales_password",
}

ODS_TABLES = ["channels", "users", "products", "visits", "orders", "order_items", "refunds"]
DWS_TABLES = [
    "dws_daily_sales_summary",
    "dws_channel_summary",
    "dws_user_value_summary",
    "dws_product_sales_summary",
    "dws_category_refund_summary",
]
ADS_TABLES = [
    "ads_daily_sales",
    "ads_channel_conversion",
    "ads_user_ltv",
    "ads_repeat_purchase",
    "ads_product_contribution",
    "ads_abnormal_refund",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local batch analytics pipeline.")
    parser.add_argument("--skip-generate", action="store_true", help="Skip PostgreSQL source data generation.")
    parser.add_argument("--skip-ods", action="store_true", help="Skip PostgreSQL to Iceberg ODS extraction.")
    parser.add_argument("--skip-dwd", action="store_true", help="Skip Iceberg DWD table build.")
    parser.add_argument("--skip-dws", action="store_true", help="Skip ClickHouse DWS computation.")
    parser.add_argument("--skip-ads", action="store_true", help="Skip ClickHouse ADS computation from DWS.")
    parser.add_argument("--run-quality-checks", action="store_true", help="Run data quality checks after ADS computation.")
    parser.add_argument("--small", action="store_true", help="Generate a smaller ODS dataset for quick tests.")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--visits", type=int, default=80000)
    parser.add_argument("--seed", type=int, default=20260618)
    parser.add_argument("--refresh-dashboard", action="store_true", help="Refresh Metabase dashboard cards/layout.")
    parser.add_argument("--metabase-email", help="Metabase admin email. Required with --refresh-dashboard.")
    parser.add_argument("--metabase-password", help="Metabase admin password. Required with --refresh-dashboard.")
    return parser.parse_args()


def run_command(command: list[str], env: dict[str, str] | None = None) -> None:
    printable = " ".join(command)
    print(f"\n>>> {printable}")
    subprocess.run(command, cwd=PROJECT_ROOT, env=env, check=True)


def spark_env() -> dict[str, str]:
    env = os.environ.copy()
    java_home = env.get("JAVA_HOME") or DEFAULT_JAVA_HOME
    env["JAVA_HOME"] = java_home
    env["PATH"] = f"{Path(java_home) / 'bin'};{env.get('PATH', '')}"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def wait_for_postgres(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with psycopg2.connect(**POSTGRES_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            print("PostgreSQL is reachable.")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("PostgreSQL is not reachable.")


def wait_for_clickhouse(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
            client.command("SELECT 1")
            print("ClickHouse is reachable.")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("ClickHouse is not reachable.")


def wait_for_minio(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urlopen("http://localhost:9002/minio/health/live", timeout=5) as response:
                if response.status == 200:
                    print("MinIO is reachable.")
                    return
        except Exception:
            time.sleep(2)
    raise RuntimeError("MinIO is not reachable.")


def postgres_counts() -> dict[str, int]:
    counts = {}
    with psycopg2.connect(**POSTGRES_CONFIG) as conn:
        with conn.cursor() as cursor:
            for table in ODS_TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = int(cursor.fetchone()[0])
    return counts


def clickhouse_counts(tables: list[str]) -> dict[str, int]:
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    return {table: int(client.query(f"SELECT count() FROM {table}").result_rows[0][0]) for table in tables}


def print_counts(title: str, counts: dict[str, int]) -> None:
    print(f"\n{title}")
    for table, count in counts.items():
        print(f"  {table:<28} {count:>10}")


def run_generate(args: argparse.Namespace) -> None:
    command = [sys.executable, str(PROJECT_ROOT / "scripts" / "generate_ods_data.py")]
    if args.small:
        command += ["--users", "500", "--products", "50", "--orders", "1000", "--visits", "3000"]
    else:
        command += [
            "--users",
            str(args.users),
            "--products",
            str(args.products),
            "--orders",
            str(args.orders),
            "--visits",
            str(args.visits),
            "--seed",
            str(args.seed),
        ]
    run_command(command)


def run_ods_extract() -> None:
    run_command([sys.executable, str(PROJECT_ROOT / "scripts" / "extract_ods_to_iceberg.py")], env=spark_env())


def run_dwd_build() -> None:
    run_command([sys.executable, str(PROJECT_ROOT / "scripts" / "build_dwd_tables.py")], env=spark_env())


def run_dws_compute() -> None:
    run_command([sys.executable, str(PROJECT_ROOT / "spark-jobs" / "compute_dws_tables.py")], env=spark_env())


def run_ads_compute() -> None:
    run_command([sys.executable, str(PROJECT_ROOT / "scripts" / "compute_ads_from_dws.py")])


def run_quality_checks() -> None:
    run_command([sys.executable, str(PROJECT_ROOT / "scripts" / "run_data_quality_checks.py")], env=spark_env())


def run_dashboard(args: argparse.Namespace) -> None:
    if not args.metabase_email or not args.metabase_password:
        raise ValueError("--metabase-email and --metabase-password are required with --refresh-dashboard.")

    run_command(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "create_metabase_dashboard.py"),
            "--email",
            args.metabase_email,
            "--password",
            args.metabase_password,
        ]
    )


def main() -> None:
    args = parse_args()
    print("Starting local batch analytics pipeline.")

    run_command(["docker", "compose", "up", "-d"])
    wait_for_postgres()
    wait_for_clickhouse()
    wait_for_minio()

    if not args.skip_generate:
        run_generate(args)
        print_counts("PostgreSQL source row counts", postgres_counts())

    if not args.skip_ods:
        run_ods_extract()

    if not args.skip_dwd:
        run_dwd_build()

    if not args.skip_dws:
        run_dws_compute()
        print_counts("DWS row counts", clickhouse_counts(DWS_TABLES))

    if not args.skip_ads:
        run_ads_compute()
        print_counts("ADS row counts", clickhouse_counts(ADS_TABLES))

    if args.run_quality_checks:
        run_quality_checks()

    if args.refresh_dashboard:
        run_dashboard(args)

    print("\nPipeline completed.")
    print("Dashboard: http://localhost:3000/dashboard/2")


if __name__ == "__main__":
    main()
