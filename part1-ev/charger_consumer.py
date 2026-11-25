# Q3 충전 세션 Consumer (그룹 ev-consumer-yso)
# 두 인스턴스를 동시에 실행하면 리밸런싱으로 파티션을 나눠 받는다.
# 실행 (EC2 호스트, 레포 루트에서): python part1-ev/charger_consumer.py 0
import json
import sys
from collections import Counter

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

TOPIC = "ev-charging-sessions"
GROUP_ID = "ev-consumer-yso"
instance = sys.argv[1] if len(sys.argv) > 1 else "0"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    group_id=GROUP_ID,
    enable_auto_commit=False,  # 자동 커밋 끄고 오프셋을 직접 관리
    auto_offset_reset="earliest",
    consumer_timeout_ms=120000,  # 2분 동안 새 메시지가 없으면 종료
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"group_id={GROUP_ID} 소비 시작 (instance #{instance})")

counts = Counter()
partitions = set()
try:
    for msg in consumer:
        # 메시지 처리: endedBy 별로 세기
        counts[msg.value["endedBy"]] += 1
        partitions.add(msg.partition)

        # 처리를 마친 메시지까지만 오프셋 반영 (다음에 읽을 위치 = offset + 1)
        tp = TopicPartition(msg.topic, msg.partition)
        consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "", -1)})
finally:
    consumer.close()

print(f"[처리 파티션] {sorted(partitions)}")
for ended_by, n in counts.most_common():
    print(f"  {ended_by}: {n}")
print(f"  합계: {sum(counts.values())}")
