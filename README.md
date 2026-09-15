# 데엔7기_김준_7주차 프로젝트
- 작성자: 김준 (데엔 7기)
- 레포지토리: https://github.com/ejmogly/de7-JuneKim-w7

## 실습 환경
- OS: macOS (Apple Silicon aarch64, Docker Desktop)
- Airflow: Docker Compose 기반 Airflow 2.8.1 (CeleryExecutor, 커스텀 이미지 week7-airflow:junekim)
- Spark: Apache Spark 3.5.7 Standalone 클러스터 (Master: 7077/9090, Worker: 8081)
- Cloud: AWS IAM & S3 (de-7-junekim)

## 회고
- 이번 7주차 프로젝트를 통해 Docker Compose를 활용한 Airflow 및 Spark 독립 클러스터 구축, 커스텀 이미지 빌드, XCom과 재시도 메커니즘, 과거 구간 백필(Backfill)을 체계적으로 실습했습니다.
- 특히 AWS IAM 최소 권한 기반 S3 연동 및 Airflow를 통한 Bronze(CSV) -> Silver(Snappy Parquet) 종단간 데이터 파이프라인을 구축하며 실무 데이터 엔지니어링 워크플로우를 깊이 체감할 수 있었습니다.
