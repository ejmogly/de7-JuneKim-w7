import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()

    # 불필요한 INFO 로그를 줄여 결과 표가 잘 보이도록 설정
    spark.sparkContext.setLogLevel("WARN")

    # 입력 경로 (컨테이너 내 /opt/spark/data/wordcount.txt)
    input_path = "data/wordcount.txt"
    if not os.path.exists(input_path):
        input_path = "/opt/spark/data/wordcount.txt"

    # 1. 텍스트 파일 읽기
    df = spark.read.text(input_path)

    # 2. 공백 기준 분리 (소문자 변환 / 구두점 제거 금지)
    # 줄바꿈/빈 줄에서 생기는 공백 문자열("") 필터링
    words = df.select(F.explode(F.split(df.value, " ")).alias("word"))
    words = words.filter(F.col("word") != "")

    # 3. 빈도 내림차순 상위 20개 .show()
    word_counts = words.groupBy("word").count().orderBy(F.col("count").desc())

    print("\n=== WordCount 빈도 내림차순 상위 20개 ===")
    word_counts.show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
