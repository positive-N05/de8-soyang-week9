# Q4 충전 세션 Spark Structured Streaming
# Kafka ev-charging-sessions -> Bronze/Silver(S3, foreachBatch) + 설치구역별 누적 세션 수(콘솔)
# 실행 (EC2 호스트, 레포 루트에서):
#   docker exec spark-master /opt/spark/bin/spark-submit \
#     --master spark://spark-master:7077 --driver-memory 768m --executor-memory 1g \
#     --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.jars.ivy=/tmp/.ivy2 \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
#     /opt/de9/part1-ev/charger_stream.py 2> ~/q4_stream.log
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

BOOTSTRAP = "kafka1:19092,kafka2:19092,kafka3:19092"
TOPIC = "ev-charging-sessions"
BUCKET = "s3a://de9-yso-datalake"
BRONZE_PATH = f"{BUCKET}/bronze/ev-charging-sessions"
SILVER_PATH = f"{BUCKET}/silver/ev-charging-sessions"
CHECKPOINT_DIR = "/opt/spark/work-dir/checkpoints"  # master·worker 공유 볼륨

# S3 는 EC2 IAM Role(인스턴스 프로파일) 자격 증명으로 접근 (액세스 키 없음)
spark = (
    SparkSession.builder.appName("ev-charging-stream-yso")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 메시지(JSON) 명시적 스키마 - Producer 가 CSV 값을 문자열로 보내므로 모두 StringType 으로 받는다
schema = StructType([
    StructField("stationName", StringType()),
    StructField("portNumber", StringType()),
    StructField("portType", StringType()),
    StructField("plugType", StringType()),
    StructField("startTime", StringType()),
    StructField("endTime", StringType()),
    StructField("totalDuration", StringType()),
    StructField("chargingDuration", StringType()),
    StructField("energyKwh", StringType()),
    StructField("endedBy", StringType()),
])


def hms_to_seconds(col_name):
    """'1:46:26' 같은 h:mm:ss 문자열을 초(int)로 변환"""
    parts = F.split(F.col(col_name), ":")
    return (parts.getItem(0).cast("int") * 3600
            + parts.getItem(1).cast("int") * 60
            + parts.getItem(2).cast("int"))


def to_location_group(col_name):
    """stationName 끝의 ' #숫자'만 제거 (예: PALO ALTO CA / WEBSTER #2 -> PALO ALTO CA / WEBSTER)"""
    return F.regexp_replace(F.col(col_name), r" #\d+$", "")


# Kafka 스트림 읽기 (트리거마다 최대 60건)
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 60)
    .load()
)


def write_bronze_silver(batch_df, batch_id):
    # ---------- Bronze: 가공 없는 원본 메시지 + Kafka 메타데이터 ----------
    bronze = (
        batch_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            "topic", "partition", "offset", "timestamp",
        )
        .withColumn("dt", F.to_date(F.get_json_object("value", "$.startTime")))
    )
    bronze.write.mode("append").partitionBy("dt").parquet(BRONZE_PATH)

    # ---------- Silver: 파싱 + locationGroup + 초 변환 + idleSec ----------
    silver = (
        bronze.select(F.from_json("value", schema).alias("e"))
        .select("e.*")
        .withColumn("locationGroup", to_location_group("stationName"))
        .withColumn("portNumber", F.col("portNumber").cast("int"))
        .withColumn("energyKwh", F.col("energyKwh").cast("double"))
        .withColumn("totalDurationSec", hms_to_seconds("totalDuration"))
        .withColumn("chargingDurationSec", hms_to_seconds("chargingDuration"))
        .withColumn("idleSec", F.col("totalDurationSec") - F.col("chargingDurationSec"))
        .withColumn("dt", F.to_date("startTime"))
    )
    silver.write.mode("append").partitionBy("dt").parquet(SILVER_PATH)


# 쿼리 1: Bronze/Silver 적재
lake_query = (
    raw.writeStream.foreachBatch(write_bronze_silver)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/ev-stream")
    .trigger(processingTime="10 seconds")
    .start()
)

# 쿼리 2: 설치구역별 누적 세션 수를 트리거마다 콘솔에 출력 (이름이 잘리지 않게 truncate=False)
location_counts = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
    .select(to_location_group("e.stationName").alias("locationGroup"))
    .groupBy("locationGroup")
    .count()
    .orderBy(F.desc("count"))
)
console_query = (
    location_counts.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/ev-console")
    .trigger(processingTime="10 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
