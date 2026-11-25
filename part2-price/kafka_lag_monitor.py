# Q10 컨슈머 그룹 lag 모니터
# 그룹 이름을 인자로 받아 파티션별 current-offset(커밋된 오프셋), log-end-offset, lag 와 lag 합계를 표로 출력한다.
# 실행 (EC2 호스트, 레포 루트에서): python part2-price/kafka_lag_monitor.py ev-consumer-yso
import sys

from kafka import KafkaAdminClient, KafkaConsumer

BOOTSTRAP = ["localhost:9092", "localhost:9093", "localhost:9094"]

if len(sys.argv) != 2:
    print("사용법: python kafka_lag_monitor.py <consumer-group>")
    sys.exit(1)
group = sys.argv[1]

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
# 끝 오프셋 조회용 Consumer (그룹에 참여하지 않고, 커밋도 하지 않는다)
consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, enable_auto_commit=False)
try:
    # 그룹이 커밋한 오프셋 {TopicPartition: OffsetAndMetadata}
    committed = admin.list_consumer_group_offsets(group)
    # 각 파티션의 마지막 오프셋 {TopicPartition: int}
    end_offsets = consumer.end_offsets(list(committed.keys()))

    print(f"group = {group}")
    print(f"{'topic':<25}{'partition':<11}{'current-offset':<16}{'log-end-offset':>14}{'lag':>8}")
    total_lag = 0
    for tp in sorted(committed, key=lambda t: (t.topic, t.partition)):
        current = committed[tp].offset
        log_end = end_offsets[tp]
        lag = log_end - current
        total_lag += lag
        print(f"{tp.topic:<25}{tp.partition:<11}{current:<16}{log_end:>14}{lag:>8}")
    print(f"{'TOTAL LAG':<25}{'':<11}{'':<16}{'':>14}{total_lag:>8}")
finally:
    consumer.close()
    admin.close()
