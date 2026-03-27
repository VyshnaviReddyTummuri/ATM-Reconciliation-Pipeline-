from kafka_producer import KafkaProducerSDK
from config import KAFKA_CONFIG
import uuid
import random
from datetime import datetime
import time

producer = KafkaProducerSDK(KAFKA_CONFIG)

while True:
    message = {
        "transaction_id": str(uuid.uuid4()),
        "atm_id": f"ATM{random.randint(1,5)}",
        "card_id": f"CARD{random.randint(1000,9999)}",
        "amount": random.randint(100, 1000),
        "transaction_time": datetime.now().isoformat()
    }

    producer.send(message)

    time.sleep(2)