# Architecture

## Goal

The project demonstrates how a simple sales business system can be extended into a learning-oriented data analytics platform.

It separates operational writes, lakehouse storage, batch computation, streaming computation, analytical serving, and dashboard visualization.

## Components

| Layer | Component | Technology | Responsibility |
|---|---|---|---|
| Business UI | Simulated sales frontend | Vue 3 + Vite | Login, browse products, create orders |
| Business API | Operational service | FastAPI | Persist orders and publish order events |
| Source database | Operational storage | PostgreSQL | Store source tables |
| Event broker | Event buffering | Kafka | Decouple order producers and stream consumers |
| Batch engine | Data processing | Apache Spark / PySpark | Extract, clean, join, aggregate |
| Lakehouse storage | Detail storage | MinIO + Apache Iceberg | Store ODS and DWD tables |
| Serving database | Analytical serving | ClickHouse | Store DWS, ADS, and RT query tables |
| BI layer | Dashboard | Metabase | Visualize historical and realtime metrics |

## Batch Path

```text
PostgreSQL
  -> Spark JDBC extraction
  -> Iceberg ODS
  -> Iceberg DWD
  -> ClickHouse DWS
  -> ClickHouse ADS
  -> Metabase
```

The batch path is used for stable historical indicators. It supports recomputation, traceability, and consistent metric definitions.

## Streaming Path

```text
Vue 3 frontend
  -> FastAPI
  -> PostgreSQL
  -> Kafka
  -> Spark Structured Streaming
  -> ClickHouse RT
  -> Metabase
```

The streaming path is used for minute-level operational feedback. A newly created order is published as an event, consumed by Spark Structured Streaming, written to ClickHouse realtime tables, and displayed in Metabase.

## Why This Architecture

A simple information system can often use only a backend service and a relational database. This project adds a data platform because analytics has different requirements:

- business writes should not be slowed down by analytical scans;
- metrics should be reusable and auditable;
- historical data should be recomputable;
- realtime events should be handled without changing batch tables;
- dashboards should query prepared analytical tables.

## Local Deployment

The project is designed for single-node development:

- Docker runs PostgreSQL, ClickHouse, Metabase, MinIO, and Kafka.
- Python runs data generation, batch jobs, quality checks, and orchestration.
- Spark runs locally through PySpark.
- The Vue frontend and FastAPI backend run as local development services.
