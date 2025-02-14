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

bash_task_2 = BashOperator(
    task_id='run_bash_command_2',
    bash_command='export DBT_PROFILES_DIR=dbt-profiles/',  # Navigate up 4 directories and then run dbt
    dag=dag,
)

# Define a bash command task
bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='cd .. && cd .. && cd opt && cd airflow && cd my_project && dbt run',  # Navigate up 4 directories and then run dbt
    dag=dag,
)

bash_task_2 >> bash_task
