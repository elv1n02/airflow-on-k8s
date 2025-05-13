from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime
import os

GITHUB_USERNAME = 'yourusername'
GITHUB_REPO = 'your-repo'
GITHUB_TOKEN = Variable.get('GITHUB_TOKEN')  # Securely stored token
CLONE_DIR = '/tmp/cloned_repo'
REPO_URL = f'https://{GITHUB_USERNAME}:{GITHUB_TOKEN}@github.com/{GITHUB_USERNAME}/{GITHUB_REPO}.git'

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

    install_git = BashOperator(
        task_id='install_git',
        bash_command='apt-get update && apt-get install -y git',
    )

    clone_repo = BashOperator(
        task_id='clone_github_repo',
        bash_command=f'rm -rf {CLONE_DIR} && git clone {REPO_URL} {CLONE_DIR}',
    )

    install_requirements = BashOperator(
        task_id='install_requirements',
        bash_command=f'pip install -r {os.path.join(CLONE_DIR, "requirements.txt")}',
    )

    run_script = BashOperator(
        task_id='run_script',
        bash_command=f'python {os.path.join(CLONE_DIR, "run.py")}',
    )

    install_git >> clone_repo >> install_requirements >> run_script
