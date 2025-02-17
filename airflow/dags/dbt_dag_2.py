from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os
import boto3

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

S3_ENDPOINT = "http://192.168.49.2:32000"
S3_ACCESS_KEY = "uKymoi0VTARafjacTuSx"
S3_SECRET_KEY = "FzANCSnL62uJGW0bpevQkLF70fQF02wUIPbIobBG"
BUCKET_NAME = "expanded-star"
LOCAL_DIRECTORY = "/opt/airflow/test_project/parquet_files/"

def upload_parquet_files_to_minio():

    # Connect to MinIO using boto3
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    for filename in os.listdir(LOCAL_DIRECTORY):
        if filename.endswith(".parquet"):
            local_file_path = os.path.join(LOCAL_DIRECTORY, filename)
            # Define the S3 object name (use the same filename or customize)
            s3_object_name = f"{filename}"
            
            # Upload the file to MinIO
            with open(local_file_path, "rb") as data:
                s3_client.upload_fileobj(data, BUCKET_NAME, s3_object_name)

            print(f"Successfully uploaded {filename} to {BUCKET_NAME}/{s3_object_name}")

dag = DAG(
    "dbt_pipeline_2",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='export DBT_PROFILES_DIR=/opt/airflow/dbt-profiles && cd .. && cd .. && cd opt && cd airflow && cd test_project && dbt run',
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_parquet_files",
    python_callable=upload_parquet_files_to_minio,
    dag=dag
)

bash_task >> upload_task
