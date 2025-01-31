from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Function to test the S3 connection
def test_s3_connection():
    s3_hook = S3Hook(aws_conn_id='minio_connection')  # Use the connection ID from Airflow Connections
    bucket_name = 'airflow'  # Replace with your S3 bucket name
    try:
        # Check if we can list objects in the bucket
        s3_objects = s3_hook.list_keys(bucket_name)
        print(f"Objects in bucket {bucket_name}: {s3_objects}")
        return f"Successfully listed objects in {bucket_name}"
    except Exception as e:
        print(f"Failed to connect to S3 or list objects: {str(e)}")
        raise Exception(f"Failed to connect to S3: {str(e)}")

# Define the DAG
dag = DAG(
    'test_s3_connection',
    description='Test S3 Connection in Airflow',
    schedule_interval=None,  # Set to None to trigger it manually
    start_date=datetime(2025, 1, 31),
    catchup=False,
)

# Create a PythonOperator to run the S3 connection test
test_s3_task = PythonOperator(
    task_id='test_s3_task',
    python_callable=test_s3_connection,
    dag=dag,
)

test_s3_task
