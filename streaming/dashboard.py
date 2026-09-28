import streamlit as st
import psycopg2
import pandas as pd
import time
from google.cloud import bigquery

st.set_page_config(page_title="Crypto Data Platform", layout="wide")
st.title("Crypto Data Platform")

tab_live, tab_historical = st.tabs(["🔴 Live Prices", "📊 Historical Analysis"])

# ---------- LIVE TAB ----------
with tab_live:
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

    live_placeholder = st.empty()

# ---------- HISTORICAL TAB ----------
with tab_historical:
    st.caption("Historical trends from the batch pipeline (Airflow → BigQuery → dbt)")

    @st.cache_data(ttl=300)  # cache for 5 minutes, since this is historical/slow-changing
    def get_historical_data():
        client = bigquery.Client(project="crypto-pipeline-project-500308")
        query = """
            SELECT
                c.coin_name,
                c.symbol,
                DATE(d.full_date) as date,
                AVG(f.current_price) as avg_price,
                AVG(f.market_cap) as avg_market_cap
            FROM `crypto_raw.fact_crypto_prices` f
            JOIN `crypto_raw.dim_coin` c ON f.coin_key = c.coin_key
            JOIN `crypto_raw.dim_date` d ON f.date_key = d.date_key
            GROUP BY c.coin_name, c.symbol, date
            ORDER BY date DESC
            LIMIT 500
        """
        return client.query(query).to_dataframe()

    hist_df = get_historical_data()

    coins = sorted(hist_df["coin_name"].unique())
    selected_coin = st.selectbox("Select a coin", coins, index=coins.index("Bitcoin") if "Bitcoin" in coins else 0)

    coin_df = hist_df[hist_df["coin_name"] == selected_coin].sort_values("date")

    st.line_chart(coin_df.set_index("date")["avg_price"], use_container_width=True)
    st.dataframe(coin_df, use_container_width=True, hide_index=True)

# ---------- LIVE REFRESH LOOP (only affects the live tab) ----------
while True:
    df = get_live_data()
    with live_placeholder.container():
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Last refreshed: {pd.Timestamp.now().strftime('%H:%M:%S')}")
    time.sleep(5)

