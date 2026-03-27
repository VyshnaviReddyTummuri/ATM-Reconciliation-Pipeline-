import os

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_SERVER", "localhost:9092"),
    "topic": "atm-transactions",
    "retries": 3
}