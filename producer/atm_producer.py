from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "atm-transactions"

transactions = [
    {"transaction_id": "TXN1001", "atm_id": "ATM001", "card_id": "CARD1001", "amount": 200, "transaction_time": "2026-03-13 10:00:00"},
    {"transaction_id": "TXN1002", "atm_id": "ATM002", "card_id": "CARD1002", "amount": 500, "transaction_time": "2026-03-13 10:01:00"},
    {"transaction_id": "TXN1003", "atm_id": "ATM003", "card_id": "CARD1003", "amount": 300, "transaction_time": "2026-03-13 10:02:00"},
    {"transaction_id": "TXN9999", "atm_id": "ATM999", "card_id": "CARD9999", "amount": 1000, "transaction_time": "2026-03-13 10:09:00"}
]

for txn in transactions:
    producer.send(topic_name, txn)
    print(f"Sent: {txn}")
    time.sleep(2)

producer.flush()