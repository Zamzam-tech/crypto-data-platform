import requests
import psycopg2
from datetime import datetime

# Database Connection Settings (Matches docker-compose.yml)
DB_CONFIG = {
    "dbname": "crypto_staging",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

def fetch_fear_greed():
    """Fetch Fear & Greed Index from alternative.me API"""
    url = "https://api.alternative.me/fng/?limit=100"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    results = []
    for entry in data['data']:
        results.append({
            'score': int(entry['value']),
            'classification': entry['value_classification'],
            'fetch_date': datetime.fromtimestamp(int(entry['timestamp'])).date(),
            'fetched_at': datetime.now().isoformat()
        })
    print(f"Fear & Greed fetched: {len(results)} records ✅")
    return results

def load_fear_greed_to_postgres():
    """Create table automatically and load data into PostgreSQL"""
    records = fetch_fear_greed()
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fear_greed_raw (
            score INT,
            classification VARCHAR(50),
            fetch_date DATE PRIMARY KEY,
            fetched_at TIMESTAMP
        )
    """)

    inserted = 0
    for record in records:
        cur.execute("""
            INSERT INTO fear_greed_raw (score, classification, fetch_date, fetched_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fetch_date) DO NOTHING
        """, (record['score'], record['classification'], record['fetch_date'], record['fetched_at']))
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    
    print(f"PostgreSQL Load complete: {inserted} records inserted into 'fear_greed_raw'. 🗄️")
import pandas as pd
from google.cloud import bigquery

BQ_PROJECT = "crypto-pipeline-project-500308"
BQ_DATASET = "crypto_raw"
BQ_TABLE = "raw_fear_greed"

def load_fear_greed_to_bigquery():
    """Read new rows from Postgres fear_greed_raw and load into BigQuery raw_fear_greed."""
    conn = psycopg2.connect(**DB_CONFIG)

    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    query = f"SELECT MAX(fetch_date) as max_date FROM `{table_ref}`"
    result = client.query(query).result()
    max_date = list(result)[0].max_date

    if max_date is None:
        sql = "SELECT * FROM fear_greed_raw"
        df = pd.read_sql(sql, conn)
    else:
        sql = "SELECT * FROM fear_greed_raw WHERE fetch_date > %(max_date)s"
        df = pd.read_sql(sql, conn, params={"max_date": max_date})

    conn.close()

    if df.empty:
        print("No new rows to load into BigQuery.")
        return

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print(f"Loaded {len(df)} new rows into {table_ref}. 🚀")
if __name__ == "__main__":
    fetch_fear_greed()
    load_fear_greed()
    load_fear_greed_to_bigquery()
