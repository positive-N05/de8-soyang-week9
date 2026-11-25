# Q7/Q8 식료품 가격 Spark 배치
# Q7: Kafka product-prices 를 한 번 읽어 Bronze(원본)/Silver(정제) 적재 + 품목별·월별 평균/최고/최저가 출력
# Q8: Silver 로 도시 × 품목별 전월(11월) 대비 당월(12월) 변동률을 윈도우 함수로 구하고, 도시 내 순위를 매겨 Gold 적재
# 실행 (EC2 호스트, 레포 루트에서):
#   docker exec spark-master /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 --driver-memory 768m --executor-memory 1g \
#     --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.jars.ivy=/tmp/.ivy2 \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
#     /opt/de9/part2-price/price_batch.py 2> ~/q7_batch.log
# (Q9 에서는 Airflow SparkSubmitOperator 가 같은 파일을 실행한다)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

BOOTSTRAP = "kafka1:19092,kafka2:19092,kafka3:19092"
TOPIC = "product-prices"
BUCKET = "s3a://de9-yso-datalake"
BRONZE_PATH = f"{BUCKET}/bronze/product-prices"
SILVER_PATH = f"{BUCKET}/silver/product-prices"
GOLD_PATH = f"{BUCKET}/gold/grocery_price_change"

# S3 는 EC2 IAM Role(인스턴스 프로파일) 자격 증명으로 접근 (액세스 키 없음)
spark = (
    SparkSession.builder.appName("price-batch-yso")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 메시지(JSON) 스키마 - Producer 가 CSV 값을 문자열로 보내므로 문자열로 받은 뒤 형 변환한다
schema = StructType([
    StructField("item_name", StringType()),
    StructField("market", StringType()),
    StructField("year", StringType()),
    StructField("month", StringType()),
    StructField("price", StringType()),
    StructField("currency", StringType()),
    StructField("unit", StringType()),
])

# ---------- 토픽에 있는 데이터를 한 번만 읽기 (배치, 처음~끝) ----------
raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# ---------- Bronze: 원본 메시지 그대로 + Kafka 메타데이터 ----------
bronze = raw.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),
    "topic", "partition", "offset", "timestamp",
)
bronze.write.mode("overwrite").parquet(BRONZE_PATH)
print(f"Bronze 적재: {spark.read.parquet(BRONZE_PATH).count()}건 -> {BRONZE_PATH}/")

# ---------- Silver: 파싱 + price 실수형 + 값이 없거나 0 이하인 레코드 제외 ----------
silver = (
    bronze.select(F.from_json("value", schema).alias("m"))
    .select("m.*")
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("month", F.col("month").cast("int"))
    .withColumn("price", F.col("price").cast("double"))
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
)
silver.write.mode("overwrite").parquet(SILVER_PATH)
silver = spark.read.parquet(SILVER_PATH)
print(f"Silver 적재: {silver.count()}건 -> {SILVER_PATH}/")

# ---------- Q7: 품목별·월별 평균가/최고가/최저가 ----------
print("=== Q7 품목별 월별 평균/최고/최저가 ===")
monthly = (
    silver.groupBy("item_name", "month")
    .agg(
        F.round(F.avg("price"), 3).alias("avg_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
    )
    .orderBy("item_name", "month")
)
monthly.show(truncate=False)

# ---------- Q8: 윈도우 함수로 전월 가격 참조 -> 변동률 -> 도시 내 순위 ----------
# 같은 도시·품목 안에서 월 순서로 정렬해 바로 앞 달(11월) 가격을 lag 로 가져온다
month_window = Window.partitionBy("market", "item_name").orderBy("year", "month")
change = (
    silver.withColumn("price_nov", F.lag("price").over(month_window))
    .filter((F.col("year") == 2016) & (F.col("month") == 12))
    .withColumnRenamed("price", "price_dec")
    .withColumn("change_rate",
                F.round((F.col("price_dec") - F.col("price_nov")) / F.col("price_nov") * 100, 2))
)

# 도시마다 변동률의 절대값이 큰 순서로 순위 (상승·하락 방향 무관, 같으면 품목명 순)
rank_window = Window.partitionBy("market").orderBy(F.abs(F.col("change_rate")).desc(), F.col("item_name"))
gold = (
    change.withColumn("change_rank", F.row_number().over(rank_window))
    .select("market", "item_name", "price_nov", "price_dec", "change_rate", "change_rank")
)
gold.write.mode("overwrite").parquet(GOLD_PATH)
gold = spark.read.parquet(GOLD_PATH)
print(f"Gold 적재: {gold.count()}건 -> {GOLD_PATH}/")

print("=== Q8 도시별 변동률 1위 ===")
gold.filter(F.col("change_rank") == 1).orderBy("market").show(truncate=False)

spark.stop()
