from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE = PROJECT_ROOT / "analytics-platform" / "scripts" / "run_batch_pipeline.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the batch pipeline repeatedly and record wall-clock duration.")
    parser.add_argument("--warmup", type=int, default=1, help="Number of warm-up runs (default 1).")
    parser.add_argument("--measured", type=int, default=3, help="Number of measured runs (default 3).")
    parser.add_argument("--small", action="store_true", help="Use the small reproducible workload.")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--visits", type=int, default=80000)
    parser.add_argument("--seed", type=int, default=20260618)
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "benchmarks" / "results" / "batch_results.json")
    return parser.parse_args()


def pipeline_command(args: argparse.Namespace) -> list[str]:
    cmd = [sys.executable, str(PIPELINE)]
    if args.small:
        cmd += ["--small"]
    else:
        cmd += [
            "--users", str(args.users),
            "--products", str(args.products),
            "--orders", str(args.orders),
            "--visits", str(args.visits),
            "--seed", str(args.seed),
        ]
    cmd += ["--run-quality-checks"]
    return cmd


def run_once(label: str, args: argparse.Namespace) -> dict:
    start = time.perf_counter()
    proc = subprocess.run(
        pipeline_command(args),
        cwd=str(PROJECT_ROOT / "analytics-platform"),
        text=True,
        capture_output=True,
    )
    elapsed = time.perf_counter() - start
    return {
        "label": label,
        "elapsed_seconds": round(elapsed, 3),
        "returncode": proc.returncode,
        "stdout_tail": "\n".join(proc.stdout.splitlines()[-20:]),
        "stderr_tail": "\n".join(proc.stderr.splitlines()[-20:]),
    }


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    results = {
        "scale": "small" if args.small else "medium",
        "config": {
            "users": 500 if args.small else args.users,
            "products": 50 if args.small else args.products,
            "orders": 1000 if args.small else args.orders,
            "visits": 3000 if args.small else args.visits,
            "seed": args.seed,
        },
        "warmup_runs": [],
        "measured_runs": [],
    }

    for i in range(args.warmup):
        print(f"[warmup {i + 1}/{args.warmup}]", flush=True)
        results["warmup_runs"].append(run_once(f"warmup-{i + 1}", args))

    for i in range(args.measured):
        print(f"[measured {i + 1}/{args.measured}]", flush=True)
        results["measured_runs"].append(run_once(f"measured-{i + 1}", args))

    durations = [r["elapsed_seconds"] for r in results["measured_runs"]]
    durations.sort()
    n = len(durations)
    median = durations[n // 2] if n % 2 else (durations[n // 2 - 1] + durations[n // 2]) / 2
    results["summary"] = {
        "count": n,
        "durations_sorted": durations,
        "median_seconds": round(median, 3),
        "min_seconds": round(min(durations), 3),
        "max_seconds": round(max(durations), 3),
    }

    args.output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
