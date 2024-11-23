from kafka import KafkaProducer
import json
import time

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'processed_logs'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def produce_logs():
    """
    Produce logs to the processed_logs topic.
    """
    logs = [
        {"level": "INFO", "message": "System started", "timestamp": time.time()},
        {"level": "WARN", "message": "High memory usage", "timestamp": time.time()},
        {"level": "ERROR", "message": "System crash detected", "timestamp": time.time()},
    ]

    for log in logs:
        print(f"Producing log: {log}")
        producer.send(TOPIC, value=log)
        time.sleep(1)

if __name__ == "__main__":
    print("Starting Log Producer...")
    produce_logs()

