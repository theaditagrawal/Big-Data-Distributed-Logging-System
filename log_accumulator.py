from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime  # Import added

# Configuration
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'logs'
OUTPUT_TOPIC = 'processed_logs'

# Kafka Consumer to read logs
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='log_accumulator_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer to forward processed logs
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_log(log):
    """
    Process raw log message to enrich or filter the data.
    """
    # Example: Add a processed timestamp
    log['processed_timestamp'] = datetime.now().isoformat()
    return log

def accumulate_logs():
    """
    Continuously process incoming logs and forward them to the output Kafka topic.
    """
    for message in consumer:
        log = message.value
        print(f"Received log: {log}")
        
        processed_log = process_log(log)
        producer.send(OUTPUT_TOPIC, processed_log)
        print(f"Forwarded processed log to {OUTPUT_TOPIC}")

if __name__ == "__main__":
    print("Starting Log Accumulator...")
    accumulate_logs()

