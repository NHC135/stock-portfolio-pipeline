import os 
import json 
import time 
import logging
from datetime import datetime, timezone

import boto3 
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

# =====================================
# config 
# =====================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_LOCAL", "http://localhost:9000")
BUCKET = os.getenv("MINIO_BUCKET", "bronzestock")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")

# =====================================
# minio 
# =====================================
s3 = boto3.client(
    "s3",
    endpoint_url = MINIO_ENDPOINT,
    aws_access_key_id= os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key= os.environ["MINIO_ROOT_PASSWORD"],
    config=Config(
        retries={"max_attempts":5, "mode": "standard"}, # auto retry brief failures
        s3={"addressing_style": "path"},
    )
)

# =====================================
# Quality Check 
# =====================================

def ensure_bucket(): 
    try: 
        s3.head_bucket(Bucket=BUCKET)
    except ClientError: 
        s3.create_bucket(Bucket=BUCKET)
        log.info("Created bucket %s", BUCKET)

def safe_json(raw): 
    """Decode a message; return None instead of crashing on bad data."""
    try: 
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError): 
        return None

def is_valid(record): 
    return( 
        isinstance(record, dict)
        and record.get("symbol")
        and record.get("c")
        and record.get("t")
    )

def object_key(record): 
    """Stable, unique key: same quote goes, to the same file, so reprocessing never duplicates"""
    symbol = record["symbol"]
    t = int(record["t"])
    day = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    return f"{symbol}/{day}/{symbol}_{t}"               # e.g. TSLA/2026-10-13/TSLA_1791576000.json

# ================================
# kafka 
# ================================

consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=KAFKA_SERVERS,
    group_id="bronze-consumer",
    enable_auto_commit=True, 
    auto_offset_reset = "earliest",
    group_id="bronze-consumer",
    value_deserializer= safe_json()
)

ensure_bucket() 
log.info("Consuming %s -> s3://%s", TOPIC, BUCKET)

try: 
    for message in consumer: 
        record = message.value 

        if not is_valid(record): 
            log.warning("Skipping invalid message (partition %s, offset %s): %r", 
                        message.partition, message.offset, record)
            consumer.commit()             
            continue 

        key = object_key(record)
        s3.put_object( 
            Bucket=BUCKET, 
            Key=key, 
            Body=json.dumps(record)
            ContentType="application/json"
        )
        consumer.commit() 
        log.info("Saved %s -> s3://%s/%s", record["Symbol"], BUCKET, key)

except KeyboardInterrupt: 
    log.info("Stopping consumer...")
finally: 
    consumer.close() 
        