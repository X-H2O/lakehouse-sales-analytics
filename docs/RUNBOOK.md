# Runbook

This runbook describes the local demonstration flow.

## Prerequisites

- Windows 11 or Linux/macOS with Docker
- Docker Desktop
- Python 3.11
- Java 17
- Node.js 20+
- Conda is optional but recommended

## 1. Start Infrastructure

```powershell
cd analytics-platform
docker compose up -d
```

Services:

```text
PostgreSQL: localhost:5432
ClickHouse: http://localhost:8123
Metabase: http://localhost:3000
MinIO console: http://localhost:9001
Kafka: localhost:9092
```

## 2. Install Python Dependencies

```powershell
conda create -n bigdata-sales python=3.11 -y
conda activate bigdata-sales
pip install -r analytics-platform/requirements.txt
pip install -r apps/business-api/requirements.txt
```

## 3. Run Batch Pipeline

```powershell
cd analytics-platform
python .\scripts\run_batch_pipeline.py --small --run-quality-checks
```

This creates source data, builds ODS/DWD/DWS/ADS layers, and runs quality checks.

## 4. Create or Refresh Metabase Dashboard

After setting up a Metabase admin account in the browser:

```powershell
cd analytics-platform
python .\scripts\create_metabase_dashboard.py --email your-email@example.com --password your-password
```

In Metabase, connect to ClickHouse using:

```text
Host: clickhouse
Port: 8123
Database: sales_ads
Username: sales_user
Password: sales_password
```

Use `clickhouse` as host because Metabase runs inside Docker Compose.

## 5. Start Realtime Stream

```powershell
cd analytics-platform
python .\spark-jobs\run_realtime_clickhouse_stream.py
```

For a one-time verification run:

```powershell
python .\spark-jobs\run_realtime_clickhouse_stream.py --trigger available-now
```

## 6. Start Business API

```powershell
cd apps\business-api
uvicorn main:app --host 127.0.0.1 --port 8001 --reload
```

## 7. Start Business Frontend

```powershell
cd apps\business-web
npm install
npm run dev
```

Open:

```text
http://127.0.0.1:5173
```

## 8. Demo Flow

1. Open the business frontend.
2. Log in with a user ID from the generated data.
3. Add products to the cart.
4. Submit an order.
5. Check PostgreSQL for the operational order.
6. Check Kafka for the order event.
7. Watch the Spark Structured Streaming log.
8. Query ClickHouse RT tables.
9. Refresh Metabase and inspect realtime dashboard cards.

## Expected Verification Results

After the main demo flow works, a reviewer should be able to observe:

- PostgreSQL contains generated source tables and newly submitted business orders.
- MinIO contains Iceberg table files for the ODS and DWD layers.
- ClickHouse contains DWS, ADS, and RT tables.
- Metabase displays historical sales metrics and realtime minute-level metrics.
- The realtime Spark job logs received, inserted, and skipped event counts.
- The quality-check script prints validation results for counts, amounts, nulls, duplicates, and rate ranges.

## Stop Services

```powershell
cd analytics-platform
docker compose down
```
