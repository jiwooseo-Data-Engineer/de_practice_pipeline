from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from metrics import save_daily_metric


default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_daily_metric(**context):
    target_date = context["ds"]
    save_daily_metric(target_date)


with DAG(
    dag_id="daily_metric_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,
    tags=["metric", "batch"],
) as dag:

    run_metric = PythonOperator(
        task_id="save_daily_metric",
        python_callable=run_daily_metric,
    )
