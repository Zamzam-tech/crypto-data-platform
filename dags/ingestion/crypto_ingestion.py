import requests
import psycopg2
from datetime import datetime, timezone

# Database Connection Settings (Postgres Container)
DB_CONFIG = {
    "dbname": "crypto_staging",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

def fetch_crypto_data():
    """Fetch and validate cryptocurrency data from CoinGecko."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not data:
        raise ValueError("CoinGecko returned no data.")

    if len(data) != 50:
        raise ValueError(f"Expected 50 records, but received {len(data)}.")

    required_fields = ["id", "symbol", "name", "current_price", "market_cap", "total_volume", "last_updated"]
    for record in data:
        missing_fields = [f for f in required_fields if f not in record]
        if missing_fields:
            raise ValueError(f"Record {record.get('id', 'unknown')} is missing: {missing_fields}")

    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in data:
        record["last_updated_at"] = record.pop("last_updated")
        record["ingested_at"] = ingested_at

    print(f"Successfully fetched and validated {len(data)} records.")
    return data

def load_crypto_data(data):
    """Create table automatically and load market data into PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_data_raw (
            id VARCHAR(100),
            symbol VARCHAR(20),
            name VARCHAR(100),
            current_price NUMERIC,
            market_cap NUMERIC,
            total_volume NUMERIC,
            last_updated_at TIMESTAMP,
            ingested_at TIMESTAMP,
            PRIMARY KEY (id, last_updated_at)
        )
    """)

    inserted = 0
    for record in data:
        cur.execute("""
            INSERT INTO crypto_data_raw (
                id, symbol, name, current_price, market_cap, total_volume, last_updated_at, ingested_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, last_updated_at) DO NOTHING
        """, (
            record['id'],
            record['symbol'],
            record['name'],
            record['current_price'],
            record['market_cap'],
            record['total_volume'],
            record['last_updated_at'],
            record['ingested_at']
        ))
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"PostgreSQL Load complete: {inserted} records merged into 'crypto_data_raw'. 🗄️")

import pandas as pd
from google.cloud import bigquery

BQ_PROJECT = "crypto-pipeline-project-500308"
BQ_DATASET = "crypto_raw"
BQ_TABLE = "raw_crypto_data"

def load_postgres_to_bigquery():
    """Read new rows from Postgres crypto_data_raw and load into BigQuery raw_crypto_data."""
    conn = psycopg2.connect(**DB_CONFIG)

    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    query = f"SELECT MAX(ingested_at) as max_ts FROM `{table_ref}`"
    result = client.query(query).result()
    max_ts = list(result)[0].max_ts

    if max_ts is None:
        sql = "SELECT * FROM crypto_data_raw"
        df = pd.read_sql(sql, conn)
    else:
        sql = "SELECT * FROM crypto_data_raw WHERE ingested_at > %(max_ts)s"
        df = pd.read_sql(sql, conn, params={"max_ts": max_ts})

    conn.close()

    if df.empty:
        print("No new rows to load into BigQuery.")
        return
# Convert Decimal columns to float64 to avoid pyarrow/BigQuery NUMERIC conversion errors
    for col in ["current_price", "market_cap", "total_volume"]:
        df[col] = df[col].astype(float)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print(f"Loaded {len(df)} new rows into {table_ref}. 🚀")

def ingest_crypto_data():
    data = fetch_crypto_data()
    load_crypto_data(data)

if __name__ == "__main__":
    ingest_crypto_data()
    load_postgres_to_bigquery()
