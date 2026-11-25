https://github.com/positive-N05/de8-soyang-week9

# 데이터 엔지니어링 9주차 과제

Kafka(EC2) 실시간 스트리밍과 S3 Bronze/Silver/Gold 배치 파이프라인입니다.
모든 구성요소(Kafka 3대, kafka-ui, Spark standalone, Airflow)는 EC2 t3.large 한 대에서 `part1-ev/docker-compose.yaml` 하나로 실행합니다.
EC2는 IAM Role `de9-ec2-yso`로 S3·Athena에 접근하며, 액세스 키는 사용하지 않습니다.

## 폴더 구조

- `part1-ev/`: 전기차 충전 세션 스트리밍 (Q1~Q5)
- `part2-price/`: 식료품 가격 배치와 Airflow (Q6~Q10)
- `data/`: 제공 CSV

## 실행 (EC2)

```bash
cd ~/de9-yso/part1-ev
docker compose up -d --build
```
