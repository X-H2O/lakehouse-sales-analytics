CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    channel_name VARCHAR(100) NOT NULL,
    channel_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    user_name VARCHAR(120) NOT NULL,
    gender VARCHAR(20) NOT NULL,
    birth_date DATE NOT NULL,
    province VARCHAR(80) NOT NULL,
    city VARCHAR(80) NOT NULL,
    registered_at TIMESTAMP NOT NULL,
    channel_id INTEGER NOT NULL REFERENCES channels(channel_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT PRIMARY KEY,
    sku VARCHAR(80) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    list_price NUMERIC(12, 2) NOT NULL,
    cost_price NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id BIGINT PRIMARY KEY,
    session_id VARCHAR(80) NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    channel_id INTEGER NOT NULL REFERENCES channels(channel_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    event_type VARCHAR(40) NOT NULL,
    visit_time TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    channel_id INTEGER NOT NULL REFERENCES channels(channel_id),
    order_time TIMESTAMP NOT NULL,
    status VARCHAR(30) NOT NULL,
    payment_method VARCHAR(40) NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    item_amount NUMERIC(14, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
    refund_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    order_item_id BIGINT NOT NULL REFERENCES order_items(order_item_id),
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    refund_time TIMESTAMP NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    reason VARCHAR(120) NOT NULL,
    status VARCHAR(30) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_channel_id ON users(channel_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_visits_visit_time ON visits(visit_time);
CREATE INDEX IF NOT EXISTS idx_visits_channel_id ON visits(channel_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_time ON orders(order_time);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_channel_id ON orders(channel_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_refunds_refund_time ON refunds(refund_time);
CREATE INDEX IF NOT EXISTS idx_refunds_product_id ON refunds(product_id);
