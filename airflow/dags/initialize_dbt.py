from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'initialize_dbt',
    description='An example DAG that runs bash commands',
    schedule_interval='@daily',  # Run daily, adjust as needed
    start_date=datetime(2025, 2, 11),
    catchup=False,
)

# Define a bash command task
bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='dbt init my_project --profile my_project',  # Your bash command here
    dag=dag,
)

bash_task
