import os 
import json 
import boto3 
import time 
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

# env variables 
aws_user = os.environ["MINIO_ROOT_USER"]
aws_password = os.environ["MINIO_ROOT_PASSWORD"]

# minio / aws connection
s3_bucket = boto3.client(
    "s3",
    endpoint_url = "http://localhost:9000",
    aws_access_key_id= aws_user ,
    aws_secret_access_key= aws_password
)

bucket_name = "bronzestock"

# define consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["host.docker.internal:29092"],
    enable_auto_commit=True,
    auto_offset_reset = "earliest", 
    group_id="bronze-consumer",
    value_deserializer= lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer streaming and saving to MinIO...")

# Main Func. 
for message in consumer: 
    record = message.value
    symbol = record.get("symbol")
    ts = record.get("fetched_at"), int(time.time())
    key = f"{symbol}/{ts}.json"

    s3_bucket.put_object( 
        Bucket=bucket_name, 
        Key=key, 
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")