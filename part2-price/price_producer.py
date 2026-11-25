# Q6 식료품 가격 Producer + 권역별 커스텀 파티셔너
# india_grocery_prices.csv 200행을 product-prices 토픽으로 보낸다. key = market(도시명)
# 실행 (EC2 호스트, 레포 루트에서): python part2-price/price_producer.py
import csv
import json
import os
from collections import Counter

from kafka import KafkaProducer
from kafka.partitioner import DefaultPartitioner

TOPIC = "product-prices"
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "india_grocery_prices.csv")


class RegionPartitioner:
    """도시가 속한 권역에 따라 파티션을 정한다. 표에 없는 도시는 기본 파티셔너 규칙을 따른다."""

    REGION_PARTITION = {
        # 0: 북부
        "Delhi": 0, "Jaipur": 0, "Lucknow": 0, "Kanpur": 0,
        # 1: 서부·중부
        "Mumbai": 1, "Ahmedabad": 1, "Bhopal": 1,
        # 2: 남부·동부
        "Hyderabad": 2, "Chennai": 2, "Patna": 2,
    }

    def __init__(self):
        self.default_partitioner = DefaultPartitioner()

    # kafka-python 은 partitioner(직렬화된 key, 전체 파티션 목록, 사용 가능한 파티션 목록) 형태로 호출한다
    def __call__(self, key_bytes, all_partitions, available_partitions):
        city = key_bytes.decode("utf-8") if key_bytes is not None else None
        if city in self.REGION_PARTITION:
            return self.REGION_PARTITION[city]
        return self.default_partitioner(key_bytes, all_partitions, available_partitions)


with open(CSV_PATH, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    acks="all",
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    retries=5,
    partitioner=RegionPartitioner(),  # send() 에서 파티션을 직접 지정하지 않는다
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

partition_counts = Counter()
try:
    for row in rows:
        # 메시지 = CSV 한 행 (item_name, market, year, month, price, currency, unit)
        future = producer.send(TOPIC, key=row["market"], value=row)
        metadata = future.get(timeout=30)  # 실제로 저장된 파티션 확인
        partition_counts[metadata.partition] += 1
finally:
    producer.flush()
    producer.close()

print(f"전송 완료: {sum(partition_counts.values())}건  -> topic={TOPIC}")
for p in sorted(partition_counts):
    print(f"  partition {p}: {partition_counts[p]}")
