from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from datetime import datetime
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
GX_DIR = os.path.abspath(os.path.join(DAG_DIR, 'gx'))
REQUIREMENTS_PATH = os.path.abspath(os.path.join(DAG_DIR, 'requirements.txt'))
RUN_PATH = os.path.abspath(os.path.join(DAG_DIR, 'run.py'))

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

    write_in_pod = KubernetesPodOperator(
        task_id="create_files_dirs",
        name="create-files-dirs",
        namespace="gx-dev",
        image="python:3.10-slim",
        cmds=["bash", "-c"],
        arguments=[
            """
            echo "Current directory: $(pwd)" && \
            mkdir -p /tmp/test_dir && \
            echo "Hello from pod" > /tmp/test_dir/hello.txt && \
            cat /tmp/test_dir/hello.txt
            """
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
    )
    
    print_dir = BashOperator(
        task_id='print_dir',
        bash_command=f'echo "DAG_DIR: {DAG_DIR}"',
    )

    print_path = BashOperator(
        task_id='print_path',
        bash_command=f'echo "REQUIREMENTS_PATH: {REQUIREMENTS_PATH}"',
    )

    install_requirements = BashOperator(
        task_id='install_requirements',
        bash_command=f'pip install -r {REQUIREMENTS_PATH}',
    )

    run_script = BashOperator(
        task_id='run_script',
        bash_command=f'python3 {RUN_PATH}',
    )

    # print_dir >> print_path >> install_requirements >> run_script
    write_in_pod
