from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

dag = DAG(
    "dbt_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='export DBT_PROFILES_DIR=/opt/airflow/dbt-profiles && cd .. && cd .. && cd opt && cd airflow && cd my_project && dbt run',
    dag=dag,
)

bash_task
