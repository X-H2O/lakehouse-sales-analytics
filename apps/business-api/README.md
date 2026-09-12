# Sales Business API

FastAPI service for the simulated sales system.

It provides login, product browsing, order creation, and recent order lookup. When an order is created, the service writes operational records to PostgreSQL and publishes a matching order event to Kafka.

## Run

```powershell
conda activate bigdata-sales
pip install -r requirements.txt
uvicorn main:app --host 127.0.0.1 --port 8001 --reload
```

## Endpoints

```text
GET  /api/health
POST /api/login
GET  /api/products
GET  /api/categories
POST /api/orders
GET  /api/orders/recent
```
