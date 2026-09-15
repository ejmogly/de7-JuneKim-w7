import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim

def main():
    parser = argparse.ArgumentParser(description="Netflix Titles Transformation via PySpark")
    parser.add_argument("--input", default="/tmp/data/netflix_titles.csv", help="Input CSV path")
    parser.add_argument("--output", default="/tmp/data/silver_output", help="Output Parquet directory path")
    parser.add_argument("--year", type=int, default=2015, help="Release year filter (>= year)")
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    min_year = args.year

    print("=" * 60)
    print("=== [과제 09] Spark 변환 작업 시작 (jobs/transform.py) ===")
    print(f" - 입력 경로: {input_path}")
    print(f" - 출력 경로: {output_path}")
    print(f" - 기준 연도: {min_year}")
    print("=" * 60)

    # 1. SparkSession 생성
    spark = SparkSession.builder \
        .appName("NetflixTransformJuneKim") \
        .getOrCreate()

    # 2. CSV 데이터 로드
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    total_raw_count = df.count()
    print(f"[1] 원본 데이터 로드 완료: 총 {total_raw_count:,} 행")

    # 3. release_year >= 기준연도 필터
    filtered_df = df.filter(col("release_year") >= min_year)
    filtered_count = filtered_df.count()
    print(f"[2] 기준 연도({min_year}년 이상) 필터링 완료: {filtered_count:,} 행")

    # 4. listed_in 장르 쉼표 분리 후 행 전개(explode) 및 공백 제거(trim)
    exploded_df = filtered_df.withColumn("genre", explode(split(col("listed_in"), ","))) \
                             .withColumn("genre", trim(col("genre")))

    # 5. type x genre 별 작품 수 집계
    aggregated_df = exploded_df.groupBy("type", "genre").count().orderBy(col("count").desc())
    agg_row_count = aggregated_df.count()
    print(f"[3] type x genre 별 작품 수 집계 완료: 총 {agg_row_count:,} 개 그룹")

    print("\n=== 상위 20개 집계 결과 미리보기 ===")
    aggregated_df.show(20, truncate=False)

    # 6. 결과 Parquet(Snappy 압축)로 저장
    print(f"\n[4] Parquet(Snappy 압축) 파일로 저장 중... -> {output_path}")
    aggregated_df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
    print("[5] Parquet 저장 성공 완료!")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()
