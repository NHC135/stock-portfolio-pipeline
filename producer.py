import os
import json 
import requests 
import time 
import logging
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

#=======================================
# config
#=======================================
API_KEY = os.environ["finnhub_api_key"]
BASE_URL = "https://finnhub.io/api/v1/quote"
KAFKA_SERVERS = os.getenv("KAFKA_BOOSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
stock_list = ['TSLA', 'NVDA', 'TSM', 'SPY', 'CME']
POLL_SECONDS = 7


# kafka connection: initial producer
producer = KafkaProducer ( 
    bootstrap_servers= KAFKA_SERVERS,
    key_serializer= lambda k: k.encode("utf-8"),                # str to bytes
    value_serializer= lambda v: json.dumps (v).encode("utf-8"), # dictionary to JSON to bytes
    acks="all",                                                 # waits till kafka stored the message first
    retries=5                                                   # tries again for the same message if fails
)

session = requests.Session() 
session.headers.update({"X-finnhub-token": API_KEY}) # hides api_key in header, and doesnt show up in logs

last_seen_t = {} # last market timestamp sent 

# retrieve data
def get_stock_info(symbol='SPY'): 
    try: 
        r = session.get(BASE_URL, params={"symbol": symbol}, timeout=10)
        r.raise_for_status()
        data = r.json()
    
    except requests.RequestException as e:    
        log.warning("Request failed for %s: %s", symbol, e)
        return None

    # ALERT: finnhub returns c=0 and t=0 for wrong ticker symbols or no data 
    if not data.get("c") or not data.get("t"): 
        log.warning("No valid quote for %s: %s", symbol, data)
        return None
    
    data["symbol"] = symbol
    data["fetched_at"] = int(time.time())
    return data

#------ prior code ----------       for get_stock_info 
# url = f"{base_url}?symbol={symbol}&token={API_KEY}" 
# r = requests.get(url)                                     
# r.raise_for_status() 
# data = r.json() 
# ------ changes made ---------- 
# - hide api key for session or error logs \
# - doesn't open a new session everytime so its 
# - requests.get() can hang forever and the producer stops silently

def on_error(exc): 
    log.error("Kafka send failed: %s", exc)

try: 
    # looping and push to kafka for streaming
    while True: 
        for symbol in stock_list: 
            quote = get_stock_info(symbol)
            if not quote:
                continue 

            # skip duplicates: when markets close over the weekend it will give the same timestamp for every poll
            if last_seen_t.get(symbol) == quote["t"]: 
                continue
            last_seen_t[symbol] = quote["t"]

            producer.send(TOPIC, key=symbol, value=quote).add_errback(on_error) # added a callback since errors get lost, messages in the buffer are dropped. 
            log.info("Sent %s @ %s", symbol, quote["c"])
            
        time.sleep(POLL_SECONDS)
except KeyboardInterrupt:
    log.info("Stopping producer...")

finally: 
    producer.flush() # send anything thats buffered
    producer.close()