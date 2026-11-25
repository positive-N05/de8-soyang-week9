# Spark 클러스터(apache/spark:3.5.4-python3, Python 3.8)에 작업을 제출할 수 있는 Airflow 이미지
# Python 버전을 Spark 클러스터와 같은 3.8로 맞춤
FROM apache/airflow:2.10.5-python3.8

# spark-submit(클라이언트 모드 드라이버)을 돌리려면 Java가 필요
USER root
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark·Amazon provider와 pyspark를 Airflow 2.10.5 공식 constraints로 설치 (pyspark 3.5.4)
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    pyspark==3.5.4 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt"
