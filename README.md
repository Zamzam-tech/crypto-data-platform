# Crypto Data Platform

An end-to-end, containerized data engineering pipeline that ingests real-time cryptocurrency and market sentiment data, stages it, loads it into a cloud data warehouse, and transforms it into a dimensional model ready for analytics.

## Architecture

```
CoinGecko API ─┐
                ├─→ Airflow (batch, every 10 min) ─→ PostgreSQL (staging) ─→ BigQuery (raw) ─→ dbt ─→ Star Schema
alternative.me ─┘        (Docker)                       (Docker)          (partitioned/clustered)   (BigQuery)
   (Fear & Greed)
```

**Layers:**
1. **Ingestion** — Python scripts fetch from the CoinGecko API (50 top coins, every 10 minutes) and the alternative.me Fear & Greed Index API (daily), orchestrated by Apache Airflow.
2. **Staging** — Raw ingested data lands in PostgreSQL first. This decouples ingestion from the warehouse, provides a fast landing zone for frequent writes, and allows replay without re-hitting rate-limited APIs.
3. **Warehouse (raw layer)** — A batch Airflow task loads new rows from Postgres into partitioned, clustered BigQuery tables (`raw_crypto_data`, `raw_fear_greed`), using a watermark (`ingested_at` / `fetch_date`) to avoid reloading old data.
4. **Transformation** — dbt builds a staging layer (light cleaning/typing) on top of the raw tables, then a dimensional (star schema) model on top of that.
5. **Orchestration** — Two Airflow DAGs (`crypto_pipeline`, `fear_greed_pipeline`) run the full chain: ingest → load to warehouse → dbt run → dbt test.

All of the above runs in Docker containers (Airflow, PostgreSQL, and — for the streaming extension — Kafka, Zookeeper, and Spark), orchestrated via Docker Compose.

## Dimensional Model

**Dimensions:**
- `dim_coin` — one row per coin, with a hashed surrogate key (`coin_key`) generated from the natural key (`coin_id`)
- `dim_date` — a generated calendar spine (2026), with a surrogate key (`date_key`) and derived attributes (day of week, month name, etc.)

**Facts:**
- `fact_crypto_prices` — grain: **one row per coin, per ingestion timestamp**. Price, market cap, and volume, joined to `dim_coin` and `dim_date` via surrogate keys.
- `fact_fear_greed` — grain: **one row per calendar day**. Market-wide sentiment score, joined to `dim_date` only (not coin-specific).

Kept as two separate fact tables rather than one combined table, since they have different natural grains (10-minute vs. daily) — joining them at the wrong grain would silently duplicate the daily sentiment score across every price row for that day.

## Design Decisions & Trade-offs

- **Surrogate keys over natural keys** — `coin_key`/`date_key` are hashed via `dbt_utils.generate_surrogate_key()` rather than joining on raw `coin_id`/dates directly. This insulates the model from upstream ID changes and is standard Kimball-style dimensional modeling practice.
- **PostgreSQL as a staging layer, not the warehouse** — Postgres is optimized for frequent small writes (ingestion); BigQuery is optimized for large analytical scans. Using each for what it's good at, rather than one tool for everything.
- **Sandbox BigQuery limitations** — this project runs on BigQuery's free sandbox tier (no billing account). This enforces a **60-day partition expiration** on tables, meaning `raw_fear_greed` cannot retain more than ~60 days of history without billing enabled. In a production setting with billing on, this would be configured for full retention.
- **Pandas over PySpark for the batch load** — at this data volume (tens to hundreds of rows per run), pandas is simpler and entirely adequate. PySpark is reserved for the streaming layer (below), where it's the right tool for continuous, higher-throughput processing.
- **NUMERIC → FLOAT64** — BigQuery's `NUMERIC` type caused repeated `pyarrow`/`Decimal` conversion failures when loading from pandas DataFrames (a known friction point in the pandas → pyarrow → BigQuery pipeline). Price/volume columns were changed to `FLOAT64`, which is adequate precision for this analytical use case.

## Streaming Extension (in progress)

A real-time ingestion path is being layered in alongside the batch pipeline, using Kafka and PySpark Structured Streaming:

```
CoinGecko API → Kafka producer (frequent polling) → Kafka topic → PySpark Structured Streaming consumer → Postgres (live table)
```

Note: CoinGecko's free API is poll-based, not push-based — "streaming" here means significantly more frequent polling treated as a message stream, not true event-driven push streaming. This is an intentional, explicit design choice, not a limitation being glossed over.

The streaming path is designed to answer a different question than the batch path: "what's happening right now" (low-latency point lookups) vs. "what happened over time" (historical, aggregated analysis) — and is intended to land in its own serving table/dashboard rather than being merged into the batch tables.

## Known Limitations / Not Yet Production-Grade

This project demonstrates the *architecture* of a production pipeline but is missing several things a real production system would need:
- No alerting (Slack/email/PagerDuty on failure)
- No monitoring/observability dashboards (pipeline health, data freshness, anomaly detection)
- Secrets (GCP key, DB credentials) are stored as local files/env vars, not in a secrets manager
- No CI/CD for pipeline code changes
- No dev/staging/prod environment separation
- No tested backfill/reprocessing strategy
- Running on BigQuery's sandbox tier (no billing), which caps partition retention at 60 days

## Tech Stack

Python · Apache Airflow · PostgreSQL · Google BigQuery · dbt · Docker & Docker Compose · Apache Kafka · Apache Spark (PySpark) · Streamlit

## Local Setup

```bash
docker compose up -d --build
```

Airflow UI: `localhost:8080`
Trigger a DAG manually: `docker exec -it airflow airflow dags trigger crypto_pipeline`
