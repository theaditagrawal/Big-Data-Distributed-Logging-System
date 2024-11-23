from kafka import KafkaConsumer
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'processed_logs'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alerting_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def monitor_logs_for_alerts():
    """
    Monitors logs and generates alerts for critical logs.
    """
    for message in consumer:
        log = message.value
        # Ensure the 'level' key exists in the log
        if 'level' in log:
            if log['level'] in ['ERROR', 'WARN']:
                print(f"ALERT: Critical log detected: {log}")
        else:
            print(f"Invalid log format: {log}")

if __name__ == "__main__":
    print("Starting Alerting System...")
    monitor_logs_for_alerts()

