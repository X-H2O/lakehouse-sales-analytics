from __future__ import annotations

import json
import time
from pathlib import Path

import psycopg2
import clickhouse_connect

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PG = dict(host="localhost", port=5432, dbname="sales_ods", user="sales_user", password="sales_password")
CH = dict(host="localhost", port=8123, database="sales_ads", username="sales_user", password="sales_password")

QUERIES = {
    "daily_sales": {
        "pg": "SELECT DATE(order_time)::text AS sale_date, COUNT(*) AS paid_orders, COUNT(DISTINCT user_id) AS paid_users, ROUND(SUM(total_amount),2) AS sales_amount FROM orders WHERE status='paid' GROUP BY DATE(order_time) ORDER BY sale_date",
        "ch": "SELECT sale_date, paid_orders, paid_users, sales_amount FROM sales_ads.dws_daily_sales_summary ORDER BY sale_date",
    },
    "channel_summary": {
        "pg": "SELECT c.channel_id, c.channel_name, (SELECT COUNT(*) FROM visits v WHERE v.channel_id=c.channel_id) AS visits, (SELECT COUNT(*) FROM orders o WHERE o.channel_id=c.channel_id AND o.status='paid') AS paid_orders, (SELECT COALESCE(SUM(total_amount),0) FROM orders o WHERE o.channel_id=c.channel_id AND o.status='paid') AS sales_amount FROM channels c ORDER BY c.channel_id",
        "ch": "SELECT channel_id, channel_name, visits, paid_orders, sales_amount FROM sales_ads.dws_channel_summary ORDER BY channel_id",
    },
    "user_value": {
        "pg": "SELECT user_id, COUNT(*) AS paid_orders, ROUND(SUM(total_amount),2) AS total_amount, MIN(DATE(order_time))::text AS first_date, MAX(DATE(order_time))::text AS last_date FROM orders WHERE status='paid' GROUP BY user_id ORDER BY total_amount DESC LIMIT 20",
        "ch": "SELECT user_id, paid_orders, total_amount, first_order_date, last_order_date FROM sales_ads.dws_user_value_summary ORDER BY total_amount DESC LIMIT 20",
    },
    "product_sales": {
        "pg": "SELECT p.product_id, p.product_name, p.category, SUM(oi.quantity) AS sold_quantity, ROUND(SUM(oi.item_amount),2) AS sales_amount FROM order_items oi JOIN products p ON p.product_id=oi.product_id GROUP BY p.product_id, p.product_name, p.category ORDER BY sales_amount DESC LIMIT 20",
        "ch": "SELECT product_id, product_name, category, sold_quantity, sales_amount FROM sales_ads.dws_product_sales_summary ORDER BY sales_amount DESC LIMIT 20",
    },
    "category_refund": {
        "pg": "SELECT p.category, COUNT(r.refund_id) AS refund_count, ROUND(SUM(r.refund_amount),2) AS refund_amount FROM refunds r JOIN products p ON p.product_id=r.product_id WHERE r.status='approved' GROUP BY p.category ORDER BY p.category",
        "ch": "SELECT category, refund_count, refund_amount FROM sales_ads.dws_category_refund_summary ORDER BY category",
    },
}


def time_query(fn, n=3):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    return {"median_ms": round(times[len(times)//2], 2), "min_ms": round(min(times), 2), "max_ms": round(max(times), 2)}


def main():
    results = {"scale": "small", "note": "PostgreSQL runs aggregation on raw ODS tables on-the-fly; ClickHouse serves precomputed DWS tables.", "queries": {}}

    with psycopg2.connect(**PG) as pg_conn:
        with pg_conn.cursor() as cur:
            for name, q in QUERIES.items():
                cur.execute(q["pg"])
                rows = cur.fetchall()
                results["queries"][name] = {"pg": time_query(lambda: (cur.execute(q["pg"]), cur.fetchall())), "pg_rows": len(rows)}

    ch = clickhouse_connect.get_client(**CH)
    for name, q in QUERIES.items():
        r = ch.query(q["ch"]).result_rows
        results["queries"][name]["ch"] = time_query(lambda: ch.query(q["ch"]).result_rows)
        results["queries"][name]["ch_rows"] = len(r)

    print(json.dumps(results, indent=2, ensure_ascii=False))

    out = PROJECT_ROOT / "benchmarks" / "results" / "postgres_baseline.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()