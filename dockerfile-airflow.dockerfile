# Dockerfile-airflow
FROM apache/airflow:2.10.5

# Switch to airflow user first
USER airflow

# Install dbt packages
RUN pip install --no-cache-dir boto3 dbt-core dbt-snowflake snowflake-connector-python python-dotenv