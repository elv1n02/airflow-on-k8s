from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from io import BytesIO

# Function to fetch data from MySQL and convert to Parquet format
def mysql_to_parquet_and_store_in_minio():
    # MySQL connection hook
    mysql_hook = MySqlHook(mysql_conn_id='mysql_connection')  # Use your connection ID here
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    
    # Query to get data from MySQL table
    query = "SELECT * FROM users"  # Change with your table name and query
    cursor.execute(query)
    
    # Fetch the data
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]  # Get column names
    df = pd.DataFrame(data, columns=columns)
    
    # Convert DataFrame to Parquet format
    table = pa.Table.from_pandas(df)
    
    # Store the Parquet data in a BytesIO buffer
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)  # Move to the beginning of the buffer
    
    # MinIO connection hook (Airflow's S3Hook can also be used for MinIO)
    s3_hook = S3Hook(aws_conn_id='minio_connection')  # Use your MinIO connection ID
    bucket_name = 'airflow'  # MinIO bucket name
    object_name = 'users.parquet'  # Path in the MinIO bucket
    
    # Upload the Parquet file to MinIO
    s3_hook.load_file_obj(parquet_buffer, object_name, bucket_name, replace=True)
    
    print(f"Parquet file successfully uploaded to {bucket_name}/{object_name}")

# Define the DAG
dag = DAG(
    'mysql_to_minio_users',
    description='Extract data from MySQL, convert to Parquet, and store in MinIO',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=datetime(2025, 1, 31),
    catchup=False,
)

# Create a PythonOperator to run the function
mysql_to_parquet_task = PythonOperator(
    task_id='mysql_to_parquet_task',
    python_callable=mysql_to_parquet_and_store_in_minio,
    dag=dag,
)

mysql_to_parquet_task
