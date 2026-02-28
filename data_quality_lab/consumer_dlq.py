from kafka import KafkaConsumer
import json

DLQ_TOPIC = "transactions-dlq"

consumer = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="dirty-dlq-consumer-group",
    api_version=(2, 0, 2),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[START] Monitoring DLQ: {DLQ_TOPIC}")

for msg in consumer:
    e = msg.value
    print("\n--- DEAD LETTER EVENT ---")
    print(f"Reason: {e.get('reason')}")
    print(f"Metadata: {e.get('meta')}")
    print(f"Raw Data:  {e.get('raw')}")
