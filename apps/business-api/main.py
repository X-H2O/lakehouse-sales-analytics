from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from schemas import CreateOrderRequest, LoginRequest, LoginResponse, OrderOut, ProductOut, RecentOrderOut
from services.auth_service import login
from services.order_service import create_order, recent_orders
from services.product_service import list_categories, list_products


app = FastAPI(title="Sales Business API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/api/login", response_model=LoginResponse)
def login_api(payload: LoginRequest):
    return login(payload.user_id)


@app.get("/api/products", response_model=list[ProductOut])
def products_api(limit: int = 30, category: str | None = None):
    return list_products(limit=limit, category=category)


@app.get("/api/categories", response_model=list[str])
def categories_api():
    return list_categories()


@app.post("/api/orders", response_model=OrderOut)
def create_order_api(payload: CreateOrderRequest):
    return create_order(payload)


@app.get("/api/orders/recent", response_model=list[RecentOrderOut])
def recent_orders_api(limit: int = 20):
    return recent_orders(limit=limit)
