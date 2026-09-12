from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    user_id: int = Field(gt=0)


class UserOut(BaseModel):
    user_id: int
    user_name: str
    province: str
    city: str
    channel_id: int


class LoginResponse(BaseModel):
    token: str
    user: UserOut


class ProductOut(BaseModel):
    product_id: int
    sku: str
    product_name: str
    category: str
    brand: str
    list_price: Decimal


class OrderItemIn(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0, le=99)


class CreateOrderRequest(BaseModel):
    user_id: int = Field(gt=0)
    channel_id: int = Field(default=1, gt=0)
    items: list[OrderItemIn] = Field(min_length=1)


class OrderItemOut(BaseModel):
    order_item_id: int
    product_id: int
    product_name: str
    quantity: int
    unit_price: Decimal
    item_amount: Decimal


class OrderOut(BaseModel):
    order_id: int
    user_id: int
    channel_id: int
    status: str
    total_amount: Decimal
    kafka_run_id: str
    items: list[OrderItemOut]


class RecentOrderOut(BaseModel):
    order_id: int
    user_id: int
    user_name: str
    order_time: str
    status: str
    total_amount: Decimal
