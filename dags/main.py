from crypto_ingestion import fetch_crypto_data, load_crypto_data
from fear_greed_ingestion import fetch_fear_greed, load_fear_greed

if __name__=="__main__":
    fetch_crypto_data()
    load_crypto_data()
    fetch_fear_greed()
    load_fear_greed()   