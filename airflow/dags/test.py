from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Function to test MySQL connection
def test_mysql_connection():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_connection')  # Use the connection ID from above
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT 1;")  # Basic query to test the connection
    result = cursor.fetchone()
    print(f"Connection Test Result: {result}")

# Define the DAG
dag = DAG(
    'test_mysql_connection',
    description='Test MySQL Connection in Airflow',
    schedule_interval=None,  # Set to None to run it manually
    start_date=datetime(2025, 1, 31),
    catchup=False,
)

# Create a PythonOperator to run the connection test
test_mysql_task = PythonOperator(
    task_id='test_mysql_task',
    python_callable=test_mysql_connection,
    dag=dag,
)

test_mysql_task
