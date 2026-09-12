# Analytics Platform

This directory contains the infrastructure, schemas, batch jobs, streaming jobs, and data quality scripts.

## Main Commands

```powershell
docker compose up -d
python .\scripts\run_batch_pipeline.py --small --run-quality-checks
python .\spark-jobs\run_realtime_clickhouse_stream.py
```

## Important Tables

- PostgreSQL source tables: `channels`, `users`, `products`, `visits`, `orders`, `order_items`, `refunds`
- Iceberg ODS tables: source-aligned copies of operational tables
- Iceberg DWD tables: cleaned analytical detail tables
- ClickHouse DWS tables: reusable subject summaries
- ClickHouse ADS tables: dashboard-facing historical metrics
- ClickHouse RT tables: realtime events and minute summaries

## Notes

The scripts use local demo defaults. For a real deployment, move credentials to environment variables or a secret manager.
