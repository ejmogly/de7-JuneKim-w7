import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "junekim",
    "start_date": datetime(2026, 9, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "de-7-junekim")
BRONZE_KEY = "bronze/netflix_titles.csv"
CONTAINER_DATA_DIR = "/tmp/data"
LOCAL_CSV_PATH = f"{CONTAINER_DATA_DIR}/netflix_titles.csv"
LOCAL_OUTPUT_DIR = f"{CONTAINER_DATA_DIR}/silver_output"


def get_s3_client():
    """Airflow Connection(aws_default) 또는 환경변수를 통해 boto3 s3 클라이언트를 안전하게 반환 (키 하드코딩 금지)"""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id="aws_default")
        client = hook.get_conn()
        if client:
            return client
    except Exception:
        pass

    import boto3
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"),
    )


def download_bronze_from_s3():
    """Task 1: boto3로 S3 bronze CSV 컨테이너 다운로드"""
    print(f"=== [Task 1] S3 Bronze 데이터 다운로드 시작 (s3://{BUCKET_NAME}/{BRONZE_KEY}) ===")
    os.makedirs(CONTAINER_DATA_DIR, exist_ok=True)
    
    s3 = get_s3_client()
    s3.download_file(BUCKET_NAME, BRONZE_KEY, LOCAL_CSV_PATH)
    
    file_size = os.path.getsize(LOCAL_CSV_PATH)
    print(f"[SUCCESS] CSV 다운로드 완료: {LOCAL_CSV_PATH} ({file_size:,} bytes)")


def upload_silver_to_s3(**kwargs):
    """Task 3: boto3로 Parquet 결과를 s3://{버킷}/silver/{오늘날짜}/ 에 업로드"""
    execution_date_str = kwargs.get("ds", datetime.now().strftime("%Y-%m-%d"))
    s3_prefix = f"silver/{execution_date_str}"
    
    print(f"=== [Task 3] S3 Silver 데이터 업로드 시작 (s3://{BUCKET_NAME}/{s3_prefix}/) ===")
    s3 = get_s3_client()
    
    if not os.path.exists(LOCAL_OUTPUT_DIR):
        raise FileNotFoundError(f"출력 디렉터리가 존재하지 않습니다: {LOCAL_OUTPUT_DIR}")
    
    uploaded_files = []
    for root, _, files in os.walk(LOCAL_OUTPUT_DIR):
        for file in files:
            # 숨김 파일이나 체크섬 crc 제외하고 parquet 및 _SUCCESS 메타 파일 업로드
            if file.startswith(".") or file.endswith(".crc"):
                continue
            local_file = os.path.join(root, file)
            s3_key = f"{s3_prefix}/{file}"
            s3.upload_file(local_file, BUCKET_NAME, s3_key)
            size = os.path.getsize(local_file)
            uploaded_files.append((s3_key, size))
            print(f"  - 업로드 완료: s3://{BUCKET_NAME}/{s3_key} ({size:,} bytes)")
            
    print(f"[SUCCESS] 총 {len(uploaded_files)}개 파일 Silver 영역 업로드 성공!")


with DAG(
    dag_id="weekly_pipeline_junekim",
    default_args=default_args,
    schedule="@weekly",
    catchup=False,
    tags=["과제09", "Q9", "S3", "Spark"],
) as dag:

    # Task 1: boto3로 S3 bronze CSV 다운로드
    task_download = PythonOperator(
        task_id="download_bronze_csv",
        python_callable=download_bronze_from_s3,
    )

    # Task 2: spark-submit으로 jobs/transform.py 실행
    task_spark = BashOperator(
        task_id="spark_transform_job",
        bash_command=(
            "spark-submit --master local[*] /opt/airflow/dags/jobs/transform.py "
            f"--input {LOCAL_CSV_PATH} "
            f"--output {LOCAL_OUTPUT_DIR} "
            "--year 2015"
        ),
    )

    # Task 3: boto3로 결과를 s3://{버킷}/silver/{오늘날짜}/에 업로드
    task_upload = PythonOperator(
        task_id="upload_silver_parquet",
        python_callable=upload_silver_to_s3,
    )

    task_download >> task_spark >> task_upload
