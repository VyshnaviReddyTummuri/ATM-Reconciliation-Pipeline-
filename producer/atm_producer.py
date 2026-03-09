from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

atm_ids = ["ATM001", "ATM002", "ATM003"]
cards = ["CARD1001", "CARD1002", "CARD1003", "CARD1004"]

while True:
    transaction = {
        "atm_id": random.choice(atm_ids),
        "card_id": random.choice(cards),
        "amount": random.choice([100, 200, 300, 500]),
        "timestamp": time.time()
    }

    producer.send("atm_transactions", transaction)
    print("Sent:", transaction)

    time.sleep(2)