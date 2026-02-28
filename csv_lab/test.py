import csv
import time
from pathlib import Path
from kafka import KafkaProducer

class Producer:
    def __init__(self, topic):
        self.topic = topic
        self.kafka_producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: v.encode('utf-8')
        )

    def send_csv_rows(self, file_path, delay=0.01, flush_interval=50):
        """Send CSV rows to Kafka gradually with periodic flush."""
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)
            count = 0
            for row in reader:
                message = ','.join(row)
                self.kafka_producer.send(self.topic, value=message)
                print(f"Sent row: {row}")
                count += 1
                if count % flush_interval == 0:
                    self.kafka_producer.flush()  # flush every `flush_interval` messages
                time.sleep(delay)
            self.kafka_producer.flush()  # final flush



BASE_DIR = Path(__file__).resolve().parent
data_file = BASE_DIR / 'data' / 'transactions.csv'

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    producer.send("csv-topic", b"hello from windows")
    producer.flush()
    print("OK")