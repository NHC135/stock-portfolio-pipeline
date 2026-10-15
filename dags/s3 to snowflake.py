import os 
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

BUCKET="bronzestock"
TABLE="bronze_stock_quotes_raw"
LOCAL_DIR="/tmp/minio_downloads" # abs path for Airflow 

# =================================
# getting data from s3 bucket
# =================================
def download_from_minio(): 
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client( 
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )

    local_files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"] 
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} => {local_file}")
            local_files.append(local_file)
    return local_files 

# ====================================
# load the files onto snowflake DW
# ====================================
def load_to_snowflake(**kwargs): 
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio') 
    if not local_files: 
        print("No files to load.") 
        return 

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DB"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    ) 

    try: 
        with conn.cursor() as cur:  
            for f in local_files: 
                cur.execute(f"PUT 'file://{f}' @%{TABLE}")
                print(f"Uploaded {f} to Snowflake stage")

            cur.execute(f""" 
                COPY INTO {TABLE}
                FROM @%{TABLE}
                FILE_FORMAT = (TYPE=JSON)
            """)
            print("COPY INTO executed", cur.fetchall())
    finally:
        conn.close() 

# ===================================
# Airflow 
# ===================================

default_args = { 
   "owner": "airflow",
   "depends_on_past": False, 
   "start_date": datetime(2026, 10, 4),
   "retries": 1, 
   "retry_delay": timedelta(minutes=5), 
}

with DAG( 
    dag_id="minio_to_snowflake", 
    default_args=default_args,
    schedule_interval="*/1 * * * *", # every 1 minutes
    catchup=False,
    max_active_runs=1
) as dag: 

    task1= PythonOperator( 
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2= PythonOperator( 
        task_id="load_snowflake",
        python_callable=load_to_snowflake, 
    )

    task1 >> task2