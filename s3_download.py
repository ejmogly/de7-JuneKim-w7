import os
import sys
import csv
import boto3

def main():
    # 1. 버킷명 주입 (실행 인자 우선, 환경변수/기본값 사용, 키 하드코딩 금지)
    if len(sys.argv) > 1:
        bucket_name = sys.argv[1]
    else:
        bucket_name = os.environ.get("S3_BUCKET_NAME", "de-7-junekim")

    print("=" * 60)
    print(f"=== [과제 08] S3 연동 및 데이터 다운로드 (버킷: {bucket_name}) ===")
    print("=" * 60)

    # 2. boto3 S3 클라이언트 생성 (로컬 aws configure 자격증명 자동 사용)
    s3 = boto3.client("s3")

    # 3. 버킷 내 객체 목록 및 크기 출력
    print("\n[1] S3 버킷 객체 목록 및 크기:")
    response = s3.list_objects_v2(Bucket=bucket_name)
    contents = response.get("Contents", [])

    if not contents:
        print("    버킷에 파일이 존재하지 않습니다.")
        return

    for item in contents:
        print(f"    - Key: {item['Key']} | Size: {item['Size']:,} bytes | LastModified: {item['LastModified']}")

    # 4. project/data/ 로 다운로드 (bronze/netflix_titles.csv 우선 탐색)
    target_key = None
    if len(sys.argv) > 2:
        target_key = sys.argv[2]
    else:
        for item in contents:
            if item["Key"] == "bronze/netflix_titles.csv":
                target_key = item["Key"]
                break
        if not target_key:
            for item in contents:
                if item["Key"].endswith(".csv"):
                    target_key = item["Key"]
                    break
        if not target_key:
            target_key = "bronze/netflix_titles.csv"

    file_name = os.path.basename(target_key)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(current_dir, "data")
    os.makedirs(download_dir, exist_ok=True)
    local_file_path = os.path.join(download_dir, file_name)

    print(f"\n[2] S3 객체 '{target_key}' 다운로드 진행 중...")
    s3.download_file(bucket_name, target_key, local_file_path)
    file_size = os.path.getsize(local_file_path)
    print(f"    - 저장 경로: {local_file_path}")
    print(f"    - 다운로드 완료 크기: {file_size:,} bytes")

    # 5. CSV 레코드 행 수 출력
    print("\n[3] 다운로드된 CSV 파일 검증:")
    with open(local_file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        data_rows = list(reader)
        row_count = len(data_rows)

    print(f"    - 헤더 컬럼 ({len(header)}개): {header[:5]} ... 등 총 {len(header)}개 컬럼")
    print(f"    - 데이터 레코드 수(헤더 제외): {row_count:,} 행")
    print(f"    - 상위 3행 미리보기:")
    for idx, row in enumerate(data_rows[:3], 1):
        print(f"      [{idx}] {row[0]}, {row[1]}, {row[2]} ({row[7]})")
    print("=" * 60)


if __name__ == "__main__":
    main()
