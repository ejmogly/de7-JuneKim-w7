import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# 실행별 산출물 생성: logical date(ds: YYYY-MM-DD)를 파일명에 반영하여 컨테이너 내 저장
def create_daily_file(**context):
    ds = context["ds"]  # YYYY-MM-DD 형식의 logical date
    file_path = f"/tmp/daily_{ds}.txt"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"Logical Date: {ds}\n")
        f.write(f"Execution Time: {context['ts']}\n")
        f.write("Status: Backfill SUCCESS\n")

    print(f"=== [backfill_task] 파일 생성 완료: {file_path} ===")


with DAG(
    dag_id="backfill_demo_junekim",
    default_args=default_args,
    start_date=datetime(2026, 9, 8),    # 작업일(2026-09-15) 기준 7일 전 고정 리터럴
    schedule_interval="@daily",         # 매일 1회 스케줄
    catchup=True,                       # 과거 7일 구간 자동 백필 실행
    tags=["Q7", "Backfill", "Catchup"],
) as dag:

    backfill_task = PythonOperator(
        task_id="create_daily_file_task",
        python_callable=create_daily_file,
    )
