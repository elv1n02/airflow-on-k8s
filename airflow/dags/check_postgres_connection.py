from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime
import os

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='private_github_clone_install_run',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Install Git, clone private GitHub repo, install requirements, and run script',
) as dag:

    install_requirements = BashOperator(
        task_id='install_requirements',
        bash_command=f'pip install -r {os.path.join("requirements.txt")}',
    )

    run_script = BashOperator(
        task_id='run_script',
        bash_command=f'python {os.path.join("run.py")}',
    )

    install_requirements >> run_script
