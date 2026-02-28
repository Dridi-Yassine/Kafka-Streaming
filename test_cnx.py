from kafka import KafkaProducer
import sys

print("Testing connection to localhost:9092...")
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092', request_timeout_ms=5000)
    print("Successfully connected to Kafka broker!")
    producer.close()
    sys.exit(0)
except Exception as e:
    print(f"Failed to connect: {e}")
    sys.exit(1)