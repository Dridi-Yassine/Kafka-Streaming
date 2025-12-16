import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'transactions_csv',  # même topic que ton producer
    bootstrap_servers='localhost:9092',
    group_id='json-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode('utf-8')  # PAS json.loads ici
)

print("JSON Consumer started...\n")

count = 0
valid = 0
invalid = 0

for message in consumer:
    count += 1
    raw_value = message.value

    try:
        event = json.loads(raw_value)
        valid += 1

        print(
            f"[{count}] VALID | "
            f"partition={message.partition} | "
            f"offset={message.offset} | "
            f"event={event}"
        )

    except json.JSONDecodeError:
        invalid += 1
        print(
            f"[{count}] INVALID (not JSON) | "
            f"partition={message.partition} | "
            f"offset={message.offset} | "
            f"value={raw_value}"
        )
