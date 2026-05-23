from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import uuid

KAFKA_BOOTSTRAP_SERVER = "localhost:9094"
TOPIC_NAME = "bank-transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

countries = ["Germany", "France", "Afghanistan", "Turkey", "Netherlands"]
transaction_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]

while True:
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": "ACC-" + str(random.randint(1000, 9999)),
        "amount": round(random.uniform(10, 10000), 2),
        "transaction_type": random.choice(transaction_types),
        "country": random.choice(countries),
        "timestamp": datetime.now().isoformat()
    }

    producer.send(TOPIC_NAME, transaction)
    producer.flush()

    print("Sent:", transaction)

    time.sleep(2)