from kafka import KafkaProducer
import json
import time 
import requests

producer=KafkaProducer(bootstrap_servers='kafka:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), api_version=(2, 5, 0))
def fetch_and_send():
    try:
        url="https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False
        }
        response=requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data=response.json()

        for coin in data:
            message = {
                "id": coin["id"],
                "symbol": coin["symbol"],
                "name": coin["name"],
                "current_price": coin["current_price"],
                "market_cap":coin["market_cap"],
                "total_volume": coin["total_volume"],
                "last_updated_at": coin["last_updated"],
                "ingested_at": time.time()
            }
            producer.send('crypto_prices', value=message)
        producer.flush()
        print(f"Sent {len(data)} price updates to Kafka.")
    except Exception as e:
        print(f"Error during fetch/send: {e}. Skipping this cycle.")

if __name__ == "__main__":
    while True:
        fetch_and_send()
        time.sleep(30)
