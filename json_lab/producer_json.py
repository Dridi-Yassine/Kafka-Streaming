#write your json producer code here
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    # acks='all',                     # wait for broker acknowledgment
    # linger_ms=0,                    # disable batching

)

topic = 'transactions_csv'

with open("transactions.json","r") as file:
    data = json.load(file) 

    for line in data:
        message = json.dumps(line).encode("utf-8")
        future = producer.send(topic, message)
        print("Sent:", message)
        time.sleep(0.1)  # simulate delay between messages

producer.flush()
producer.close()