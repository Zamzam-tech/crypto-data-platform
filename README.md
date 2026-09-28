# Crypto Data Platform

An end-to-end, containerized data platform that ingests cryptocurrency and market sentiment data through **two parallel pipelines** — a scheduled batch pipeline and a real-time streaming pipeline — each feeding a purpose-built destination, with a live dashboard on top of both and CI/CD checks on every push.

## Architecture

```
BATCH (Airflow, two DAGs)

  CoinGecko API  ──►  crypto_pipeline DAG (every 10 min)
                        → PostgreSQL (staging) → BigQuery raw_crypto_data ──┐
                                                                            │
  alternative.me ──►  fear_greed_pipeline DAG (daily)                       │
   (Fear & Greed)       → PostgreSQL (staging) → BigQuery raw_fear_greed ───┤
                                                                            ▼
                        dbt → Star Schema (dim_coin, dim_date,
                              fact_crypto_prices, fact_fear_greed)

                    ┌───────────────────── STREAMING ──────────────────────┐
CoinGecko API ──────┤  Kafka Producer (every 30s) → Kafka topic            │
                     │        → PySpark Structured Streaming               │
                     │        → PostgreSQL (live_crypto_data, UPSERT)      │
                     └────────────────────────────────────────────────────┘

                     Streamlit Dashboard (Live tab + Historical tab)
                     reading from both destinations
```

Everything runs in Docker containers — Airflow (with dbt installed), PostgreSQL, Kafka, Zookeeper, and Spark (master + worker) — orchestrated via Docker Compose.

## Why two pipelines, not one

The batch and streaming paths intentionally answer **different questions** and write to **different destinations** — this was a deliberate design choice, not two versions of the same thing:

- **Batch → BigQuery** answers "how has this changed over time" — historical, aggregated, analytical. This is what a data warehouse is built for.
- **Streaming → PostgreSQL (UPSERT)** answers "what is it right now" — a single current-state row per coin, overwritten on every update, never growing. This is a point-lookup problem, not an analytical-scan problem, so it deliberately does **not** feed BigQuery — querying a warehouse for "give me the latest value" at high frequency is the wrong tool for that job.

## Dimensional Model (batch)

**Dimensions:**
- `dim_coin` — one row per coin, with a hashed surrogate key (`coin_key`)
- `dim_date` — a generated calendar spine, with a surrogate key (`date_key`)

**Facts:**
- `fact_crypto_prices` — grain: **one row per coin, per ingestion timestamp**
- `fact_fear_greed` — grain: **one row per calendar day**

Kept as two separate fact tables since they have different natural grains (10-minute vs. daily); combining them would silently duplicate the daily sentiment score across every price row for that day.

## Data Retention

`raw_fear_greed` in BigQuery holds the full history returned by the alternative.me API — every value is present and none are dropped or expired by the BigQuery sandbox tier. Loads use a `fetch_date` watermark, so each run only appends days newer than what the table already holds.

## Streaming Pipeline

```
CoinGecko API → Kafka producer (polls every 30s) → Kafka topic (crypto_prices)
              → PySpark Structured Streaming consumer (schema-validated JSON parsing)
              → PostgreSQL live_crypto_data (INSERT ... ON CONFLICT (id) DO UPDATE)
```

- CoinGecko's API is poll-based, not push-based — "streaming" here means frequent polling treated as a message stream, not true event-driven push streaming. This is a deliberate, stated design choice.
- `live_crypto_data` uses `id` as its primary key, so every new message **overwrites** the existing row for that coin rather than appending — the table stays at a fixed size (one row per tracked coin) and always reflects the latest known price.
- The producer runs as its own long-lived Docker service, decoupled from Airflow — Airflow orchestrates scheduled batch work; the producer is a continuously-running process, which is a different operational pattern and intentionally kept separate.

## Dashboard

A single Streamlit app with two tabs:
- **Live Prices** — auto-refreshes every 5 seconds, reads directly from `live_crypto_data` in PostgreSQL
- **Historical Analysis** — reads from the BigQuery star schema, lets you select a coin and view its price trend over time, cached for 5 minutes since historical data doesn't need per-second freshness

## CI/CD

A GitHub Actions workflow runs automatically on every push, so broken changes are caught before they reach the running pipelines. Checks include:
- **Python syntax validation** — catches Python errors before they reach Airflow or the streaming pipeline
- **Docker image builds** — verifies the Airflow, producer, and Spark images can be built successfully
- **`docker compose config` validation** — catches YAML, interpolation, and configuration errors in the Compose file

Credentials never live in the repo: secrets are loaded from a git-ignored `.env` file locally, and a `.env.example` documents the variables required.

Deployment is intentionally out of scope — this is a solo project running locally, with no staging or production servers to deploy to.

## Design Decisions & Trade-offs

- **Surrogate keys over natural keys** — `coin_key`/`date_key` are hashed via `dbt_utils.generate_surrogate_key()` rather than joining on raw `coin_id`/dates directly, insulating the model from upstream ID changes (standard Kimball-style dimensional modeling).
- **PostgreSQL as a staging layer, not the warehouse** — optimized for frequent small writes (ingestion); BigQuery is optimized for large analytical scans. Each tool used for what it's good at.
- **PostgreSQL (UPSERT) over Redis for the live-price store** — Redis is the more common production choice for this pattern, but at this scale, a Postgres table with `ON CONFLICT DO UPDATE` achieves the same result without introducing a new piece of infrastructure. Worth noting as a scaling trade-off, not an oversight.
- **Pandas over PySpark for the batch load** — at batch's data volume (tens to hundreds of rows per run), pandas is simpler and adequate. PySpark is reserved for the streaming layer, where it's the right tool for continuous processing.
- **NUMERIC → FLOAT64** — BigQuery's `NUMERIC` type caused repeated `pyarrow`/`Decimal` conversion failures loading from pandas DataFrames. Price/volume columns were changed to `FLOAT64`, adequate precision for this analytical use case.

## Known Limitations / Not Yet Production-Grade

This project demonstrates the *architecture* of a production pipeline but is missing several things a real production system would need:
- No alerting on streaming pipeline failures (batch has email alerts; streaming does not yet)
- No consumer lag monitoring for Kafka
- No monitoring/observability dashboards (pipeline health, data freshness, anomaly detection)
- No dev/staging/prod environment separation
- No tested backfill/reprocessing strategy
- Runs on BigQuery's free sandbox tier (no billing account)
- The Spark streaming consumer is run manually via `spark-submit`, not as a persistent service — unlike the producer, which runs continuously in its own container

## Tech Stack

Python · Apache Airflow · PostgreSQL · Google BigQuery · dbt · Docker & Docker Compose · Apache Kafka · Apache Spark (PySpark Structured Streaming) · Streamlit · GitHub Actions

## Local Setup

```bash
cp .env.example .env   # fill in your own credentials
docker compose up -d --build
```

Airflow UI: `localhost:8080`
Trigger a batch DAG manually: `docker exec -it airflow airflow dags trigger crypto_pipeline`

Run the dashboard:
```bash
streamlit run streaming/dashboard.py
```
