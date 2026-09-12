# Data Model and Metrics

## Source Tables

| Table | Purpose |
|---|---|
| `channels` | Sales and traffic channels such as App, Web, ads, affiliate, and offline store |
| `users` | User profile and registration information |
| `products` | Product catalog, category, brand, list price, and cost price |
| `visits` | Simulated user behavior events before purchase |
| `orders` | Order header records, including user, channel, status, time, and total amount |
| `order_items` | Order line items, including product, quantity, unit price, and item amount |
| `refunds` | Refund records with reason, status, amount, and time |

## Data Layers

| Layer | Storage | Meaning | Example Output |
|---|---|---|---|
| ODS | Iceberg on MinIO | Source-aligned data copied from PostgreSQL | `ods_orders`, `ods_order_items` |
| DWD | Iceberg on MinIO | Cleaned and joined detail data | `dwd_order_detail`, `dwd_visit_detail` |
| DWS | ClickHouse | Reusable summary tables | `dws_daily_sales_summary` |
| ADS | ClickHouse | Dashboard-facing metric tables | `ads_daily_sales`, `ads_user_ltv` |
| RT | ClickHouse | Realtime event and minute-level tables | `rt_order_events`, `rt_order_minute_summary` |

## Dashboard Metrics

| Metric | Question | Computation |
|---|---|---|
| Daily sales | How much revenue was generated each day? | Sum paid order amount by date |
| Channel conversion | Which channel converts better? | Paid orders divided by channel visits or events |
| Customer lifetime value | Which users contribute the most revenue? | Sum paid order amount by user |
| Repeat purchase rate | How many paid users buy again? | Users with at least two paid orders divided by paid users |
| Product contribution | Which products contribute the most sales? | Sum paid item amount by product |
| Abnormal refund rate | Which categories show refund anomalies? | Refund order count divided by paid order count by category |
| Latest realtime order events | Did the newest order event reach the pipeline? | Read newest rows from `rt_order_events` |
| Minute realtime summary | Are minute-level metrics updated? | Aggregate RT events by event-time minute |

## Quality Checks

The quality script checks:

- row counts between layers;
- duplicate primary keys and duplicate event IDs;
- null business keys;
- negative or missing order amounts;
- amount reconciliation between DWD, DWS, and ADS;
- rate ranges such as conversion and refund percentage.

These checks are intentionally simple, but they make the dashboard easier to trust and debug.
