from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# 1. 파이썬 작업 1 (문자열 return - print 아님)
def python_step_1():
    return "Step 1 completed successfully"

# 2. 파이썬 작업 2 (문자열 return - print 아님)
def python_step_2():
    return "Step 2 completed successfully"

with DAG(
    dag_id="sample_dag",
    default_args=default_args,
    schedule_interval="@daily",  # 매일 1회 스케줄
    catchup=False,               # 과거 구간 자동 실행 방지
    tags=["Q3", "과제03"],
) as dag:

    # 시작: EmptyOperator (동작 없음)
    start_task = EmptyOperator(task_id="start")

    # 파이썬 작업 2개
    task_1 = PythonOperator(
        task_id="python_task_1",
        python_callable=python_step_1,
    )

    task_2 = PythonOperator(
        task_id="python_task_2",
        python_callable=python_step_2,
    )

    # 종료: EmptyOperator (동작 없음)
    end_task = EmptyOperator(task_id="end")

    # 총 4개 task 순차 연결
    start_task >> task_1 >> task_2 >> end_task
