from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
AP = ROOT / "analytics-platform"
PG = dict(host="localhost", port=5432, dbname="sales_ods", user="sales_user", password="sales_password")


def run_py(script: str):
    return subprocess.run([sys.executable, str(script)], cwd=AP, text=True, capture_output=True)


def inject_faults():
    faults = []
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE orders SET total_amount = -100.00 WHERE order_id = (SELECT MIN(order_id) FROM orders WHERE status='paid')")
            faults.append({"id": "negative_order_total", "injected": cur.rowcount})
            cur.execute("UPDATE order_items SET item_amount = -30.00 WHERE order_item_id = (SELECT MIN(order_item_id) FROM order_items)")
            faults.append({"id": "negative_item_amount", "injected": cur.rowcount})
            cur.execute("UPDATE order_items SET unit_price = -5.00 WHERE order_item_id = (SELECT MIN(order_item_id) FROM order_items WHERE order_item_id > (SELECT MIN(order_item_id) FROM order_items))")
            faults.append({"id": "negative_unit_price", "injected": cur.rowcount})
            cur.execute("UPDATE refunds SET refund_amount = -50.00 WHERE refund_id = (SELECT MIN(refund_id) FROM refunds WHERE status='approved')")
            faults.append({"id": "negative_refund_amount", "injected": cur.rowcount})
    return faults


def main():
    print("=== 1) generate clean data ===")
    g = run_py(AP / "scripts" / "generate_ods_data.py")
    print("generate rc=", g.returncode)

    print("=== 2) inject faults ===")
    faults = inject_faults()
    for f in faults:
        print(f"  injected {f['id']} rows={f['injected']}")

    print("=== 3) run pipeline (extract -> dwd -> dws -> ads) ===")
    for step in ["scripts/extract_ods_to_iceberg.py", "scripts/build_dwd_tables.py",
                 "spark-jobs/compute_dws_tables.py", "scripts/compute_ads_from_dws.py"]:
        r = run_py(AP / step)
        print(f"  {step} rc={r.returncode}")

    print("=== 4) run data quality checks ===")
    q = run_py(AP / "scripts" / "run_data_quality_checks.py")
    fail_lines = [l for l in q.stdout.splitlines() if l.startswith("[FAIL]")]
    summary_line = next((l for l in q.stdout.splitlines() if l.startswith("Summary")), "Summary: n/a")
    print(summary_line)
    for l in fail_lines:
        print("  ", l)

    results = {
        "injected_faults": faults,
        "quality_summary": summary_line,
        "failed_checks": fail_lines,
        "failed_check_count": len(fail_lines),
    }
    out = ROOT / "benchmarks" / "results" / "fault_injection.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()