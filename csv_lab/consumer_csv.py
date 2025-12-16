"""
consumer_csv.py
----------------
Kafka Consumer that reads raw CSV rows from a topic.

Each message is treated as a simple string.
Kafka does NOT interpret CSV structure.
"""

import time
from kafka import KafkaConsumer


class CSVConsumer:
    def __init__(
        self,
        topic,
        bootstrap_servers="localhost:9092",
        group_id="csv-consumer-group",
        auto_offset_reset="earliest"
    ):


        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda v: v.decode("utf-8")
        )

    def start(self):
        """
        Continuously reads messages from Kafka.
        """
        print("Starting CSV Consumer...\n")

        for message in self.consumer:
            print(
                f"Received: {message.value} | "
                f"partition={message.partition} | "
                f"offset={message.offset}"
            )

            # Simulate processing time
            time.sleep(0.5)


if __name__ == "__main__":

    TOPIC = "transactions_csv"

    consumer = CSVConsumer(
        topic=TOPIC,
        group_id="csv-consumer-group"
    )

    consumer.start()
