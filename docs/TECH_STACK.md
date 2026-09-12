# Technology Stack

## Core Stack

| Technology | Role | Why It Is Used |
|---|---|---|
| PostgreSQL | Operational database | Stores source business records |
| Apache Spark | Batch and stream processing engine | Provides a layered data processing model for learning batch and streaming workflows |
| PySpark | Python API for Spark | Allows Spark jobs to be written in Python |
| Apache Iceberg | Lakehouse table format | Adds table metadata, schema, and snapshot management over files |
| MinIO | S3-compatible object storage | Provides local object storage for Iceberg tables |
| Kafka | Event broker | Buffers and distributes order events |
| Spark Structured Streaming | Streaming computation | Consumes Kafka events and writes realtime analytical tables |
| ClickHouse | OLAP serving database | Supports fast analytical queries for dashboards |
| Metabase | BI dashboard | Displays historical and realtime metrics |
| FastAPI | Backend framework | Implements the simulated business API |
| Vue 3 + Vite | Frontend framework | Implements the simulated business UI |

## Learning Value

This stack shows the difference between a normal information system and a data platform:

- a normal system focuses on transactions;
- a data platform focuses on traceability, recomputation, metric consistency, analytical speed, and data freshness;
- batch and streaming pipelines can coexist in the same architecture.

## Production Notes

The current project is a local prototype. A production version would need:

- secure secret management;
- container orchestration;
- monitoring and alerting;
- stronger data lineage;
- access control;
- repeatable performance testing;
- backup and disaster recovery.
