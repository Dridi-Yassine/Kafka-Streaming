from kafka import KafkaConsumer
import sys

consumer = KafkaConsumer(
    'transactions-csv',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='lab-consumer-group',
    api_version=(2, 0, 2),
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Consumer ready to listen (Part I - Raw CSV)...")
print(f"Subscribed topics: {consumer.subscription()}")

print("Waiting for messages...")

for message in consumer:
    print(f"received msg (raw) : {message.value}")
    sys.stdout.flush()
