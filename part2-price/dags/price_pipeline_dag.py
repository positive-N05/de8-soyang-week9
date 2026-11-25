# Q9 식료품 가격 파이프라인 DAG
# 10분마다: (1) Spark 클러스터에 price_batch.py 제출 -> (2) Athena 에 Gold 테이블 등록 -> (3) 도시별 1위 조회
# Athena 조회의 쿼리 실행 ID 는 query_top1 태스크의 XCom(return_value)으로 남는다.
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
ATHENA_DATABASE = "de9_yso"
ATHENA_WORKGROUP = "de9-yso"  # 쿼리 결과 위치(s3://de9-yso-datalake/athena-results/)가 작업 그룹에 설정돼 있음

with DAG(
    dag_id="price_pipeline_yso",
    schedule="*/10 * * * *",  # 10분 주기
    start_date=datetime(2026, 11, 25),
    catchup=False,  # 지난 구간은 몰아서 실행하지 않음
    max_active_runs=1,  # 실행이 겹치지 않게
    tags=["de9", "week9"],
) as dag:
    # Task 1: Q7·Q8 배치를 Spark standalone 클러스터에 제출 (spark_default = spark://spark-master:7077)
    run_price_batch = SparkSubmitOperator(
        task_id="run_price_batch",
        conn_id="spark_default",
        application="/opt/de9/part2-price/price_batch.py",
        name="price-batch-yso",
        deploy_mode="client",
        packages=SPARK_PACKAGES,
        driver_memory="768m",
        executor_memory="1g",
        conf={
            "spark.driver.host": "airflow",  # 워커가 airflow 컨테이너의 드라이버로 접속
            "spark.driver.bindAddress": "0.0.0.0",
        },
    )

    # Task 2: Athena DB de9_yso 에 Q8 Gold 경로를 가리키는 테이블 등록
    create_gold_table = AthenaOperator(
        task_id="create_gold_table",
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS de9_yso.grocery_price_change (
                market string,
                item_name string,
                price_nov double,
                price_dec double,
                change_rate double,
                change_rank int
            )
            STORED AS PARQUET
            LOCATION 's3://de9-yso-datalake/gold/grocery_price_change/'
        """,
        database=ATHENA_DATABASE,
        workgroup=ATHENA_WORKGROUP,
        aws_conn_id=None,  # 액세스 키 없이 EC2 IAM Role 사용
        region_name="ap-northeast-2",
        do_xcom_push=False,
    )

    # Task 3: 도시별 변동률 1위 조회 -> 쿼리 실행 ID 가 XCom return_value 로 남는다
    query_top1 = AthenaOperator(
        task_id="query_top1",
        query="""
            SELECT market, item_name, price_nov, price_dec, change_rate, change_rank
            FROM de9_yso.grocery_price_change
            WHERE change_rank = 1
            ORDER BY market
        """,
        database=ATHENA_DATABASE,
        workgroup=ATHENA_WORKGROUP,
        aws_conn_id=None,
        region_name="ap-northeast-2",
    )

    run_price_batch >> create_gold_table >> query_top1
