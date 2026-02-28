from kafka import KafkaProducer
import json
import time
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
json_file = os.path.join(BASE_DIR, 'json_lab', 'transactions.json')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 0, 2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'transactions-json'

try:
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file) 

        for record in data:
            producer.send(topic_name, value=record)
            print(f"sent json msg : {record}")
            time.sleep(1)
except FileNotFoundError:
    print(f"Error: {json_file} not found. Please ensure you are running from the root.")

producer.flush()
producer.close()
