from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

KST = pendulum.timezone("Asia/Seoul")

def run_daily_metric(**context):
    # Airflow logical date (data interval start)의 날짜를 date 객체로
    target_date = context["data_interval_start"].in_timezone(KST).date()

    # lazy import: DAG 파싱 시 무거운 모듈 로드 방지
    from metrics import save_daily_metric
    save_daily_metric(target_date)

with DAG(
    dag_id="daily_metric_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 6, 1, tz=KST),
    schedule="@daily",
    catchup=False,          # 필요하면 True로, 단 백필 규모 주의
    max_active_runs=1,      # 중복/겹침 방지
    tags=["metric", "batch"],
) as dag:

    run_metric = PythonOperator(
        task_id="save_daily_metric",
        python_callable=run_daily_metric,
    )