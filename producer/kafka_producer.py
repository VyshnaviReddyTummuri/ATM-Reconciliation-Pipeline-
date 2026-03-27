from kafka import KafkaProducer
import json
import logging
import time

logging.basicConfig(level=logging.INFO)

class KafkaProducerSDK:

    def __init__(self, config):
        self.topic = config["topic"]
        self.retries = config.get("retries", 3)

        self.producer = KafkaProducer(
            bootstrap_servers=config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, message):
        for i in range(self.retries):
            try:
                self.producer.send(self.topic, message)
                self.producer.flush()
                logging.info(f"Message sent: {message}")
                return
            except Exception as e:
                logging.error(f"Retry {i+1} failed: {e}")
                time.sleep(2)

        logging.error("Failed after retries")