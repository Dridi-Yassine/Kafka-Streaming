from kafka import KafkaProducer
import time
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
csv_file = os.path.join(BASE_DIR, 'data', 'transactions.csv')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 0, 2),
    value_serializer=lambda v: v.encode('utf-8')
)

topic_name = 'transactions-csv'

with open(csv_file, 'r', encoding='utf-8') as file:
    # Skip header
    next(file)
    
    for line in file:
        stripped_line = line.strip()
        if stripped_line:
            producer.send(topic_name, value=stripped_line)
            print(f"msg sent (raw csv) : {stripped_line}")
            time.sleep(1)  

producer.flush()
producer.close()
