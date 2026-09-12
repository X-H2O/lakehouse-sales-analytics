from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = PROJECT_ROOT / "sql" / "01_create_ods_tables.sql"

DB_URL = "postgresql+psycopg2://sales_user:sales_password@localhost:5432/sales_ods"

CHANNELS = [
    (1, "App", "owned"),
    (2, "Web", "owned"),
    (3, "Mini Program", "owned"),
    (4, "Douyin Ads", "paid_ads"),
    (5, "Search Ads", "paid_ads"),
    (6, "Affiliate", "partner"),
    (7, "Offline Store", "offline"),
    (8, "Email", "owned"),
]

CATEGORIES = ["Phone", "Laptop", "Audio", "Home", "Beauty", "Sports", "Food", "Book"]
BRANDS = ["Northline", "Aster", "BluePeak", "Movo", "Sunrail", "Kinoko", "UrbanNest", "Flow"]
PAYMENT_METHODS = ["alipay", "wechat_pay", "credit_card", "debit_card"]
VISIT_EVENTS = ["view_product", "add_to_cart", "favorite", "search_click"]
REFUND_REASONS = ["quality_issue", "wrong_size", "late_delivery", "changed_mind", "damaged_package"]

PROVINCE_CITIES = {
    "北京市": ["北京市"],
    "上海市": ["上海市"],
    "天津市": ["天津市"],
    "重庆市": ["重庆市"],
    "河北省": ["石家庄市", "唐山市", "保定市", "廊坊市"],
    "山西省": ["太原市", "大同市", "运城市", "临汾市"],
    "辽宁省": ["沈阳市", "大连市", "鞍山市", "锦州市"],
    "吉林省": ["长春市", "吉林市", "四平市", "延边市"],
    "黑龙江省": ["哈尔滨市", "齐齐哈尔市", "牡丹江市", "大庆市"],
    "江苏省": ["南京市", "苏州市", "无锡市", "南通市"],
    "浙江省": ["杭州市", "宁波市", "温州市", "嘉兴市"],
    "安徽省": ["合肥市", "芜湖市", "蚌埠市", "安庆市"],
    "福建省": ["福州市", "厦门市", "泉州市", "漳州市"],
    "江西省": ["南昌市", "赣州市", "九江市", "上饶市"],
    "山东省": ["济南市", "青岛市", "烟台市", "潍坊市"],
    "河南省": ["郑州市", "洛阳市", "开封市", "南阳市"],
    "湖北省": ["武汉市", "宜昌市", "襄阳市", "荆州市"],
    "湖南省": ["长沙市", "株洲市", "岳阳市", "衡阳市"],
    "广东省": ["广州市", "深圳市", "佛山市", "东莞市"],
    "海南省": ["海口市", "三亚市", "儋州市", "琼海市"],
    "四川省": ["成都市", "绵阳市", "德阳市", "乐山市"],
    "贵州省": ["贵阳市", "遵义市", "六盘水市", "安顺市"],
    "云南省": ["昆明市", "大理市", "丽江市", "曲靖市"],
    "陕西省": ["西安市", "咸阳市", "宝鸡市", "渭南市"],
    "甘肃省": ["兰州市", "天水市", "酒泉市", "庆阳市"],
    "青海省": ["西宁市", "海东市", "格尔木市"],
    "台湾省": ["台北市", "高雄市", "台中市", "台南市"],
    "内蒙古自治区": ["呼和浩特市", "包头市", "赤峰市", "鄂尔多斯市"],
    "广西壮族自治区": ["南宁市", "柳州市", "桂林市", "北海市"],
    "西藏自治区": ["拉萨市", "日喀则市", "林芝市"],
    "宁夏回族自治区": ["银川市", "吴忠市", "固原市"],
    "新疆维吾尔自治区": ["乌鲁木齐市", "克拉玛依市", "喀什市", "伊宁市"],
    "香港特别行政区": ["香港"],
    "澳门特别行政区": ["澳门"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate local ODS sales data into PostgreSQL.")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--visits", type=int, default=80000)
    parser.add_argument("--seed", type=int, default=20260618)
    return parser.parse_args()


def execute_schema(engine) -> None:
    schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")
    truncate_sql = """
        TRUNCATE TABLE
            refunds,
            order_items,
            orders,
            visits,
            products,
            users,
            channels
        RESTART IDENTITY CASCADE;
    """
    with engine.begin() as conn:
        conn.execute(text(schema_sql))
        conn.execute(text(truncate_sql))


def random_time(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def make_channels() -> pd.DataFrame:
    return pd.DataFrame(CHANNELS, columns=["channel_id", "channel_name", "channel_type"])


def make_users(fake: Faker, count: int, start: datetime, end: datetime) -> pd.DataFrame:
    rows = []
    for user_id in range(1, count + 1):
        registered_at = random_time(start, end - timedelta(days=15))
        birth_year = random.randint(1970, 2005)
        province = random.choice(list(PROVINCE_CITIES.keys()))
        city = random.choice(PROVINCE_CITIES[province])
        rows.append(
            {
                "user_id": user_id,
                "user_name": fake.name(),
                "gender": random.choice(["female", "male", "unknown"]),
                "birth_date": datetime(birth_year, random.randint(1, 12), random.randint(1, 28)).date(),
                "province": province,
                "city": city,
                "registered_at": registered_at,
                "channel_id": random.choices(
                    [channel[0] for channel in CHANNELS],
                    weights=[18, 16, 14, 18, 12, 10, 6, 6],
                    k=1,
                )[0],
            }
        )
    return pd.DataFrame(rows)


def make_products(count: int, start: datetime) -> pd.DataFrame:
    rows = []
    for product_id in range(1, count + 1):
        category = random.choice(CATEGORIES)
        list_price = round(random.uniform(19, 4999), 2)
        cost_price = round(list_price * random.uniform(0.45, 0.78), 2)
        rows.append(
            {
                "product_id": product_id,
                "sku": f"SKU-{product_id:06d}",
                "product_name": f"{random.choice(BRANDS)} {category} {product_id:04d}",
                "category": category,
                "brand": random.choice(BRANDS),
                "list_price": list_price,
                "cost_price": cost_price,
                "created_at": random_time(start - timedelta(days=180), start),
            }
        )
    return pd.DataFrame(rows)


def make_visits(users: pd.DataFrame, products: pd.DataFrame, count: int, start: datetime, end: datetime) -> pd.DataFrame:
    rows = []
    user_ids = users["user_id"].tolist()
    product_ids = products["product_id"].tolist()
    channel_ids = [channel[0] for channel in CHANNELS]
    for visit_id in range(1, count + 1):
        rows.append(
            {
                "visit_id": visit_id,
                "session_id": f"S{random.randint(1, max(1, count // 3)):010d}",
                "user_id": random.choice(user_ids),
                "channel_id": random.choices(channel_ids, weights=[20, 18, 15, 16, 12, 9, 4, 6], k=1)[0],
                "product_id": random.choice(product_ids),
                "event_type": random.choices(VISIT_EVENTS, weights=[70, 14, 8, 8], k=1)[0],
                "visit_time": random_time(start, end),
            }
        )
    return pd.DataFrame(rows)


def make_orders_and_items(
    users: pd.DataFrame,
    products: pd.DataFrame,
    order_count: int,
    start: datetime,
    end: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = []
    items = []
    product_lookup = products.set_index("product_id")["list_price"].to_dict()
    product_ids = list(product_lookup.keys())
    user_ids = users["user_id"].tolist()
    channel_ids = [channel[0] for channel in CHANNELS]
    order_item_id = 1

    for order_id in range(1, order_count + 1):
        user_id = random.choice(user_ids)
        channel_id = random.choices(channel_ids, weights=[18, 16, 14, 17, 13, 10, 6, 6], k=1)[0]
        order_time = random_time(start, end)
        status = random.choices(["paid", "cancelled", "closed"], weights=[86, 8, 6], k=1)[0]
        item_count = random.choices([1, 2, 3, 4], weights=[62, 25, 10, 3], k=1)[0]
        total_amount = 0.0
        current_items = []

        for product_id in random.sample(product_ids, item_count):
            unit_price = round(product_lookup[product_id] * random.uniform(0.75, 1.0), 2)
            quantity = random.choices([1, 2, 3], weights=[78, 17, 5], k=1)[0]
            item_amount = round(unit_price * quantity, 2)
            total_amount += item_amount
            current_items.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "item_amount": item_amount,
                }
            )
            order_item_id += 1

        if status != "paid":
            total_amount = 0.0
            for item in current_items:
                item["item_amount"] = 0.0

        orders.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "order_time": order_time,
                "status": status,
                "payment_method": random.choice(PAYMENT_METHODS),
                "total_amount": round(total_amount, 2),
            }
        )
        items.extend(current_items)

    return pd.DataFrame(orders), pd.DataFrame(items)


def make_refunds(orders: pd.DataFrame, order_items: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    paid_orders = orders[orders["status"] == "paid"][["order_id", "user_id", "order_time"]]
    paid_items = order_items.merge(paid_orders, on="order_id", how="inner")
    product_category = products.set_index("product_id")["category"].to_dict()
    rows = []
    refund_id = 1

    for item in paid_items.itertuples(index=False):
        category = product_category[item.product_id]
        base_rate = 0.055
        anomaly_boost = 0.12 if category in {"Beauty", "Audio"} else 0.0
        refund_rate = base_rate + anomaly_boost
        if random.random() > refund_rate:
            continue

        refund_time = item.order_time + timedelta(days=random.randint(1, 21), hours=random.randint(0, 23))
        refund_amount = round(float(item.item_amount) * random.uniform(0.5, 1.0), 2)
        rows.append(
            {
                "refund_id": refund_id,
                "order_id": item.order_id,
                "order_item_id": item.order_item_id,
                "user_id": item.user_id,
                "product_id": item.product_id,
                "refund_time": refund_time,
                "refund_amount": refund_amount,
                "reason": random.choice(REFUND_REASONS),
                "status": random.choices(["approved", "rejected", "processing"], weights=[78, 12, 10], k=1)[0],
            }
        )
        refund_id += 1

    return pd.DataFrame(rows)


def write_table(engine, table_name: str, df: pd.DataFrame) -> None:
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    print(f"Inserted {len(df):>7} rows into {table_name}")


def main() -> None:
    args = parse_args()
    random.seed(args.seed)
    fake = Faker("zh_CN")
    Faker.seed(args.seed)

    end = datetime(2026, 6, 18, 0, 0, 0)
    start = end - timedelta(days=180)

    engine = create_engine(DB_URL)
    execute_schema(engine)

    channels = make_channels()
    users = make_users(fake, args.users, start, end)
    products = make_products(args.products, start)
    visits = make_visits(users, products, args.visits, start, end)
    orders, order_items = make_orders_and_items(users, products, args.orders, start, end)
    refunds = make_refunds(orders, order_items, products)

    write_table(engine, "channels", channels)
    write_table(engine, "users", users)
    write_table(engine, "products", products)
    write_table(engine, "visits", visits)
    write_table(engine, "orders", orders)
    write_table(engine, "order_items", order_items)
    write_table(engine, "refunds", refunds)

    print("ODS data generation completed.")


if __name__ == "__main__":
    main()
