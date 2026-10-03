import os
import json 
import requests 
import time 
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

api_key = os.environ["api_key"]
base_url = "https://finnhub.io/api/v1/quote"

stock_list = ['TSLA', 'NVDA', 'TSMC', 'SPY', 'CME']

# kafka connection: initial producer
producer = KafkaProducer ( 
    bootstrap_servers= ["host.docker.internal:29092"],
    value_serializer= lambda x: json.dumps(x).encode("utf-8") # from bytes to json
)

# retrieve data
def get_stock_info(symbol='SPY'): 
    url = f"{base_url}?symbol={symbol}&token={api_key}" 
    try: 
        r = requests.get(url)
        r.raise_for_status() 
        data = r.json() 
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:    
        print(f"Error fetching {symbol}: {e}")
        return None

# looping and push to kafka for streaming
while True: 
    for symbol in stock_list: 
        quote = get_stock_info(symbol)
        if quote: 
            print(f"Producing: {quote}")
            producer.send("stock-quotes", value=quote)
    time.sleep(7)