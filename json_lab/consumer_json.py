from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions-json',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='json-lab-group',
    api_version=(2, 0, 2),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 0, 2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_totals = defaultdict(float)
HIGH_VALUE_THRESHOLD = 400.0
DERIVED_TOPIC = 'transactions-high-value'

print("Consumer ready (Part II - JSON & Topic Derivation)...")

for message in consumer:
    try:
        data = message.value
        user_id = data.get('user_id')
        amount = float(data.get('amount', 0))

        # Update totals
        user_totals[user_id] += amount
        print(f"user_id={user_id} | total={user_totals[user_id]:.2f}")

        # Topic Derivation: Route high-value transactions
        if amount > HIGH_VALUE_THRESHOLD:
            print(f"!!! High-value transaction detected: {amount}. Routing to {DERIVED_TOPIC}")
            producer.send(DERIVED_TOPIC, value=data)
            producer.flush()

    except Exception as e:
        print(f"Error processing message: {e}")
