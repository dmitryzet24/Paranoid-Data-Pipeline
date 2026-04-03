# The Paranoid Data Pipeline (Anti-Tracker Edition)

A high-performance data engineering project designed to monitor, analyze, and suppress telemetry and hidden trackers using Pi-hole as a primary data source.

## Architecture
The pipeline follows a Medallion Architecture:
1.  **Bronze (Raw):** Real-time log capture from Pi-hole saved as `.parquet` files in MinIO.
2.  **Silver (Filtered):** Cleaned and typed data moved into ClickHouse/Postgres.
3.  **Gold (Analytics):** Aggregated views for anomaly detection and "Hunt" reports.

## Stage 1: Ingestion
The `producer.py` script acts as a high-speed log forwarder.

### Features
- **Asynchronous Tail:** Uses `asyncio` for non-blocking I/O.
- **Regex Parsing:** Transforms unstructured `dnsmasq` logs into structured JSON/DataFrames.
- **Micro-Batching:** Buffers logs in memory and flushes to **Parquet** to optimize storage and future query speed.

### Setup
1. **Dependencies:**
   ```bash
   pip install pandas pyarrow fastparquet
