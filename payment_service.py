import requests
import json
import time
import uuid
from datetime import datetime
import random

# Configuration
FLUENTD_HOST = 'localhost'
FLUENTD_PORT = 8887
SERVICE_NAME = 'PaymentService'  # Unique name for this service

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
        "message": f"{SERVICE_NAME} is running",
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
        "message": f"Potential delay in {SERVICE_NAME}",
        "service_name": SERVICE_NAME,
        "response_time_ms": "1200",
        "threshold_limit_ms": "1000",
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
        "message": f"Error encountered in {SERVICE_NAME}",
        "service_name": SERVICE_NAME,
        "error_details": {
            "error_code": "501",
            "error_message": "Payment processing failure"
        },
        "timestamp": datetime.now().isoformat()
    }
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(log_message))
    print(f"Sent ERROR log for {SERVICE_NAME} with node_id: {node_id}")

def send_heartbeat():
    heartbeat_message = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": "UP",  # or "DOWN", depending on service status
        "timestamp": datetime.now().isoformat()
    }
    requests.post(f'http://{FLUENTD_HOST}:{FLUENTD_PORT}/logs', data=json.dumps(heartbeat_message))
    print(f"Sent heartbeat for {SERVICE_NAME} with node_id: {node_id}")

def simulate_service_activity():
    # Simulate the service running with periodic log generation
    while True:
        send_info_log()  # Send INFO log every iteration
        send_heartbeat()  # Send heartbeat every iteration
        time.sleep(10)  # Wait 10 seconds between logs

        # Randomly send WARN and ERROR logs
        if random.choice([True, False]):
            send_warn_log()
        
        if random.choice([True, False]):
            send_error_log()

if __name__ == "__main__":
    send_registration_message()  # Send registration message when service starts
    simulate_service_activity()  # Simulate service's normal operations