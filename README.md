https://github.com/positive-N05/de8-soyang-week9

# 데이터 엔지니어링 9주차 과제

Kafka(EC2) 실시간 스트리밍과 S3 Bronze/Silver/Gold 배치 파이프라인입니다.

- 모든 구성요소(Kafka broker 3대, kafka-ui, Spark standalone master/worker, Airflow)는 **EC2 t3.large 한 대**에서 `part1-ev/docker-compose.yaml` 하나로 실행합니다.
- EC2 에는 IAM Role `de9-ec2-yso`(인라인 정책 `de9-s3-athena`)를 연결했고, 호스트·Spark·Airflow 모두 이 Role 로 S3(`de9-yso-datalake`)와 Athena(작업 그룹 `de9-yso`, DB `de9_yso`)에 접근합니다. 액세스 키는 어디에도 두지 않았습니다.
- 이니셜: `yso` (컨슈머 그룹 `ev-consumer-yso`, Athena DB `de9_yso`, DAG `price_pipeline_yso`, IAM Role `de9-ec2-yso`)

## 폴더 구조

```
part1-ev/      docker-compose.yaml, requirements.txt(kafka-python==2.3.2)
               charger_producer.py (Q2), charger_consumer.py (Q3), charger_stream.py (Q4·Q5)
               capture/Q1~Q5
part2-price/   airflow/Dockerfile (Spark 에 제출할 수 있는 Airflow 이미지)
               price_producer.py (Q6), price_batch.py (Q7·Q8), dags/price_pipeline_dag.py (Q9), kafka_lag_monitor.py (Q10)
               capture/Q6~Q10
data/          제공 CSV 2개 (수정하지 않음)
```

`data/`와 `capture/`는 공개 레포에는 올리지 않고(`.gitignore`) 제출 ZIP 에만 넣었습니다. EC2 에는 CSV 를 따로 복사했습니다.

## 실행 순서 (EC2, 레포는 `~/de9-yso`)

공통으로 쓰는 Spark 제출 옵션:

```bash
SUBMIT="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --driver-memory 768m --executor-memory 1g \
  --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
```

| 문항 | 실행 |
|---|---|
| Q1 | `cd part1-ev && docker compose up -d --build` / 토픽 생성: `docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --create --topic ev-charging-sessions --partitions 3 --replication-factor 2` |
| Q2 | `python part1-ev/charger_producer.py` (한 번만 실행) |
| Q3 | `python part1-ev/charger_consumer.py 0` 과 `... 1` 을 동시에 실행 (각자 처리한 파티션과 endedBy 건수 출력) |
| Q4·Q5 | `docker exec --env-file ~/.de9_slack.env spark-master /opt/spark/bin/spark-submit ... /opt/de9/part1-ev/charger_stream.py` (Slack Webhook 은 레포 밖 `~/.de9_slack.env` 에만 둠) |
| Q5 Athena | `CREATE DATABASE de9_yso;` / `CREATE EXTERNAL TABLE de9_yso.ev_charging_agg (locationGroup string, anomaly_count bigint) STORED AS PARQUET LOCATION 's3://de9-yso-datalake/gold/ev_charging_agg/';` |
| Q6 | 토픽 `product-prices`(파티션 3·복제 2) 생성 후 `python part2-price/price_producer.py` (한 번만 실행) |
| Q7·Q8 | `$SUBMIT /opt/de9/part2-price/price_batch.py` |
| Q9 | Airflow UI(8090)에서 `price_pipeline_yso` unpause → 10분 주기 스케줄 실행 2회 성공 확인 후 pause |
| Q10 | `docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 --group ev-consumer-yso --topic ev-charging-sessions --reset-offsets --to-earliest --execute` / `python part2-price/kafka_lag_monitor.py ev-consumer-yso` |

## 참고

- Producer(Q2·Q6)는 각각 한 번만 실행했습니다 (ev-charging-sessions 175건 = 76/35/64, product-prices 200건 = 80/60/60).
- Q4·Q5 는 첫 실행 캡처에서 실행 지문 결과가 화면에 가려져, S3 bronze/silver/gold 결과와 스트리밍 체크포인트를 모두 지운 뒤 같은 커밋으로 처음부터 다시 실행했습니다. 제출 캡처는 모두 두 번째 실행 결과입니다 (Slack 은 오후 6:58 에 온 18건).
- Q9 의 Athena 쿼리 실행 ID 는 `query_top1` 태스크의 XCom(return_value)으로 남고, Athena 콘솔 최근 쿼리의 실행 ID 와 같습니다.
