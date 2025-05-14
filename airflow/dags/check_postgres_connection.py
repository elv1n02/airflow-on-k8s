from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from airflow.models import Variable

default_args = {
    'start_date': datetime(2024, 1, 1),
}

GITHUB_PAT = Variable.get("GITHUB_TOKEN")
REPO = "elv1n02/gx-dev"  # e.g., elvinmammadov/my-private-repo
BRANCH = "main"  # or whichever branch you want

REPO_URL = f"https://{GITHUB_PAT}:x-oauth-basic@github.com/{REPO}.git"

with DAG(
    dag_id='clone_private_git_and_run_python',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_git_python = KubernetesPodOperator(
        task_id="clone_and_run",
        name="git-run",
        namespace="gx-dev",
        image="python:3.10",  # Has git + pip
        cmds=["bash", "-c"],
        arguments=[
            f"""
            apt-get update && apt-get install -y git && \
            git clone --single-branch --branch {BRANCH} {REPO_URL} repo && \
            cd repo && \
            pip install -r requirements.txt && \
            python3 run.py
            python3 upload_to_minio.py
            """
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
    )
