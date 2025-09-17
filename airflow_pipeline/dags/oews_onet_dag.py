from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator


import sys
# Add the path to your pipeline code
sys.path.append("/opt/airflow")

# ENV that your code will read
os.environ.setdefault("PIPELINE_BASE_DIR", "/opt/airflow/data")

# Import your pipeline functions
from pipeline.load_data import (
    extract_oews_data,
    transform_oews_data,
    load_oews_data,
    extract_onet_skills_data,
    transform_onet_skills_data,
    load_onet_skills_data,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="oews_onet_pipeline",
    default_args=default_args,
    description="Extract/Transform/Load for OEWS & O*NET skills",
    schedule="0 3 * * *",       # daily at 03:00
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["oews", "onet", "etl"],
) as dag:

    # --- OEWS ---
    oews_extract = PythonOperator(
        task_id="oews_extract",
        python_callable=extract_oews_data,
    )

    oews_transform = PythonOperator(
        task_id="oews_transform",
        python_callable=transform_oews_data,
    )

    oews_load = PythonOperator(
        task_id="oews_load",
        python_callable=load_oews_data,
    )

    # --- O*NET Skills ---
    onet_extract = PythonOperator(
        task_id="onet_extract",
        python_callable=extract_onet_skills_data,
    )

    onet_transform = PythonOperator(
        task_id="onet_transform",
        python_callable=transform_onet_skills_data,
    )

    onet_load = PythonOperator(
        task_id="onet_load",
        python_callable=load_onet_skills_data,
    )

    # Dependencies
    oews_extract >> oews_transform >> oews_load
    onet_extract >> onet_transform >> onet_load
