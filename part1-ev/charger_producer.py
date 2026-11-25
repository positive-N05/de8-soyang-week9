# Q2 충전 세션 Producer
# charger_sessions.csv 를 startTime 순으로 한 행씩 Kafka ev-charging-sessions 토픽에 보낸다.
# 실행 (EC2 호스트, 레포 루트에서): python part1-ev/charger_producer.py
import csv
import json
import os
import time

from kafka import KafkaProducer

TOPIC = "ev-charging-sessions"
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "charger_sessions.csv")

# 1. CSV 읽기 -> startTime 순으로 정렬
with open(CSV_PATH, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
rows.sort(key=lambda r: r["startTime"])

# 2. Producer 설정
#    acks="all": 복제본까지 저장돼야 성공 -> 브로커 1대가 죽어도 유실 없음
#    enable_idempotence=True: 재전송돼도 한 번만 저장 -> 중복 없음
#    key=stationName: 같은 충전기는 항상 같은 파티션
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    acks="all",
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    retries=5,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

sent = 0
try:
    for row in rows:
        # 메시지 = CSV 한 행 (stationName ~ endedBy 10개 필드)
        future = producer.send(TOPIC, key=row["stationName"], value=row)
        future.get(timeout=30)  # 브로커에 저장된 것을 확인한 뒤 센다
        sent += 1
        time.sleep(0.1)
finally:
    producer.flush()
    producer.close()

print(f"전송 완료: {sent}건 -> topic={TOPIC}")
