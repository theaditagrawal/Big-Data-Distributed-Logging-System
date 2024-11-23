import requests
import json
import time
import uuid
from datetime import datetime
import random
import threading

# Configuration
FLUENTD_HOST = 'localhost'
FLUENTD_PORT = 8887
SERVICE_NAME = 'UserService'  # Unique name for this service

# Generate unique node_id
node_id = str(uuid.uuid4())

def send_registration_message():
    # Prepare registration message
    registration_message = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }
    
    # Send the registration message to Fluentd
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(registration_message))
    print(f"Sent registration message for {SERVICE_NAME} with node_id: {node_id}")

def send_info_log():
    log_message = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": f"{SERVICE_NAME} is handling user requests",
        "service_name": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(log_message))
    print(f"Sent INFO log for {SERVICE_NAME} with node_id: {node_id}")

def send_warn_log():
    log_message = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "WARN",
        "message_type": "LOG",
        "message": f"{SERVICE_NAME} is facing high login attempts",
        "service_name": SERVICE_NAME,
        "login_attempts": 150,
        "threshold_limit": 100,
        "timestamp": datetime.now().isoformat()
    }
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(log_message))
    print(f"Sent WARN log for {SERVICE_NAME} with node_id: {node_id}")

def send_error_log():
    log_message = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": f"{SERVICE_NAME} encountered an error during user data retrieval",
        "service_name": SERVICE_NAME,
        "error_details": {
            "error_code": "404",
            "error_message": "User data not found"
        },
        "timestamp": datetime.now().isoformat()
    }
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(log_message))
    print(f"Sent ERROR log for {SERVICE_NAME} with node_id: {node_id}")

def send_heartbeat():
    heartbeat_message = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": "UP",  # Change to "DOWN" if the service is down
        "timestamp": datetime.now().isoformat()
    }
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(heartbeat_message))
    print(f"Sent heartbeat for {SERVICE_NAME} with node_id: {node_id}")

def simulate_service_activity():
    print("Simulating service activity...")  # Debugging statement to ensure the loop is running
    # Simulate the service running with periodic log generation
    while True:
        send_info_log()  # Send INFO log every iteration
        send_heartbeat()  # Send heartbeat every iteration
        time.sleep(10)  # Wait 10 seconds between logs

        print("Sending random WARN and ERROR logs...")  # Debugging statement
        # Randomly send WARN and ERROR logs
        if random.choice([True, False]):
            send_warn_log()
        
        if random.choice([True, False]):
            send_error_log()

def listen_for_messages():
    # Create Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',  # Start from the beginning of the topic
        enable_auto_commit=True,
        group_id='monitoring_group',  # Unique group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Listening for incoming messages...")
    
    # Process incoming messages
    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == "__main__":
    print("Heartbeat Monitor is running...")  # Ensure the script is running
    send_registration_message()  # Send registration message when service starts

    # Start a thread to simulate service activity
    threading.Thread(target=simulate_service_activity, daemon=True).start()
    
    # Start listening for incoming messages
    listen_for_messages()