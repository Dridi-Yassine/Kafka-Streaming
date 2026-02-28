from kafka import KafkaProducer
import csv
import json
import time
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_file = os.path.join(BASE_DIR, 'data', 'transactions_dirty.csv')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 0, 2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'transactions-dirty'

print(f"Starting Dirty Data Producer (Topic: {topic_name})...")

try:
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file) 

        for row in reader:
            # Check if row is empty or malformed
            if not any(row.values()):
                print(f"Skipping empty row: {row}")
                continue
                
            producer.send(topic_name, value=row)
            print(f"msg sent : {row}")
            time.sleep(1)  
except FileNotFoundError:
    print(f"Error: {csv_file} not found.")

producer.flush()
producer.close()
