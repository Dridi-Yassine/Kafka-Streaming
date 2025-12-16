#Write your csv producer code here

"""
producer.py
------------
Kafka Producer that streams CSV data line by line.

Each CSV row is sent as a raw Kafka message.
Kafka does NOT interpret CSV structure.
"""

import time
import csv
from kafka import KafkaProducer


class CSVProducer:
    def __init__(self, csv_path, topic, bootstrap_servers="localhost:9092", delay=1.0):
        self.csv_path = csv_path
        self.topic = topic
        self.delay = delay

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode("utf-8")
        )

    def start(self):

        with open(self.csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)

            # Optional: skip header
            header = next(reader)
            print(f"CSV Header: {header}")

            for row in reader:
                message = ",".join(row)

                print(f"Sending: {message}")
                self.producer.send(self.topic, value=message)

                time.sleep(self.delay)

        self.producer.flush()
        print("CSV streaming completed.")


if __name__ == "__main__":


    CSV_FILE = "transactions.csv"  # or transactions_dirty.csv
    TOPIC = "transactions_csv"

    producer = CSVProducer(
        csv_path=CSV_FILE,
        topic=TOPIC,
        delay=0.5
    )

    producer.start()
