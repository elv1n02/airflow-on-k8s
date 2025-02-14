from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag = DAG(
    'initialize_dbt',
    description='An example DAG that runs bash commands',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 11),
    catchup=False,
)

bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='export DBT_PROFILES_DIR=/opt/airflow/dbt-profiles && dbt run --profiles-dir $DBT_PROFILES_DIR',
    dag=dag,
)

bash_task
