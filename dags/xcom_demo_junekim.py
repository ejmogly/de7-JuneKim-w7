from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
}

# 1. 앞 작업: 1회차 고의 실패 -> 2회차 재시도 성공 후 값 return (XCom 자동 push)
def calculate_with_retry(**context):
    ti = context["ti"]
    try_number = ti.try_number
    print(f"=== [calculate_task] 현재 시도 횟수(try_number): {try_number} ===")

    # 첫 번째 시도(1회차)에서는 고의로 예외를 발생시켜 재시도 유도
    if try_number < 2:
        print(f"시도 {try_number}: 재시도 검증을 위해 의도적으로 에러를 발생시킵니다.")
        raise RuntimeError(f"의도적 실패 발생 (시도 {try_number})")

    # 두 번째 시도(2회차)부터 정상 계산 수행 및 반환
    calculated_value = 42 * 100  # 4200
    print(f"시도 {try_number}: 재시도 성공! 계산된 결과값: {calculated_value}")
    return calculated_value


# 2. 뒤 작업: xcom_pull로 앞 작업의 반환값을 받아 로그에 출력
def pull_and_log(**context):
    ti = context["ti"]
    pulled_value = ti.xcom_pull(task_ids="calculate_task")
    print("=" * 50)
    print(f"=== [pull_task] XCom으로 수신한 값: {pulled_value} ===")
    print("=" * 50)
    return f"Processed value: {pulled_value}"


with DAG(
    dag_id="xcom_demo_junekim",
    default_args=default_args,
    schedule_interval=None,  # 수동 트리거 실행
    catchup=False,
    tags=["Q6", "XCom", "Retry"],
) as dag:

    # 앞 작업: retries 2회 이상, 재시도 간격 5초 설정
    calculate_task = PythonOperator(
        task_id="calculate_task",
        python_callable=calculate_with_retry,
        retries=2,                          # 2회 이상 재시도 설정
        retry_delay=timedelta(seconds=5),   # 5초 후 재시도
    )

    # 뒤 작업
    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_and_log,
    )

    # 태스크 순차 연결
    calculate_task >> pull_task
