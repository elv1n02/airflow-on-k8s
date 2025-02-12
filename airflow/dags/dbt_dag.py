from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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

dbt_run = KubernetesPodOperator(
    namespace="default",
    image="dbt-core:latest",  # Use your locally built dbt image
    cmds=["dbt"],
    arguments=["run"],
    name="dbt-run",
    task_id="dbt-run-task",
    get_logs=True,
    dag=dag
)

dbt_run
