import streamlit as st
import psycopg2
import pandas as pd
import time

st.set_page_config(page_title="Live Crypto Prices", layout="wide")
st.title("🔴 Live Crypto Prices")
st.caption("Auto-refreshing every 5 seconds — powered by Kafka + PySpark Structured Streaming")

def get_live_data():
    conn = psycopg2.connect(
        dbname="crypto_staging",
        user="airflow",
        password="airflow",
        host="localhost",
        port="5433"
    )
    df = pd.read_sql("""
        SELECT id, symbol, name, current_price, market_cap, total_volume, ingested_at
        FROM live_crypto_data
        ORDER BY market_cap DESC
    """, conn)
    conn.close()
    return df

placeholder = st.empty()

while True:
    df = get_live_data()
    with placeholder.container():
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Last refreshed: {pd.Timestamp.now().strftime('%H:%M:%S')}")
    time.sleep(5)
