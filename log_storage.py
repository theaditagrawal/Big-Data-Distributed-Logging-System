from elasticsearch import Elasticsearch
import json
import datetime

# Initialize the Elasticsearch client with updated options
es = Elasticsearch(
    ["localhost:9200"],
    timeout=30,  # Optional: Set a timeout if needed
    max_retries=10,  # Optional: Set max retries
    retry_on_timeout=True
)

# Function to create an index with mappings (if it doesn't exist)
def create_log_index():
    if not es.indices.exists(index="logs"):
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                    "message": {"type": "text"},
                    "details": {"type": "text"},
                    "service": {"type": "keyword"}
                }
            }
        }
        # Create the "logs" index with mappings
        try:
            es.indices.create(index="logs", body=mapping, ignore=400)  # Ignore 400 to prevent errors if already exists
            print("Created log index with mappings.")
        except Exception as e:
            print(f"Error creating index: {e}")
    else:
        print("Log index already exists.")

# Function to store logs in Elasticsearch
def store_log(log):
    # Format the log into a dictionary to store in Elasticsearch
    log_data = {
        "timestamp": log['timestamp'],
        "level": log['level'],
        "message": log['message'],
        "details": log.get('details', ''),
        "service": log['service_name']
    }

    try:
        # Index the log into Elasticsearch
        es.index(index="logs", body=log_data)
        print(f"Log stored: {log_data}")
    except Exception as e:
        print(f"Error storing log: {e}")

# Example function to simulate generating a log and sending it to Elasticsearch
def generate_log_example():
    # Generate an example log entry
    log = {
        "timestamp": datetime.datetime.now().isoformat(),
        "level": "INFO",
        "message": "Service started successfully.",
        "details": "No issues encountered during startup.",
        "service_name": "microservice_order"
    }
    store_log(log)

# Example: Function to store multiple logs (for testing or batch processing)
def store_multiple_logs(logs):
    for log in logs:
        store_log(log)

# Main function to initialize the storage and generate logs
if __name__ == "__main__":
    create_log_index()  # Ensure the index is created first

    # Simulate generating and storing logs
    generate_log_example()

    # Example batch of logs
    batch_logs = [
        {
            "timestamp": datetime.datetime.now().isoformat(),
            "level": "ERROR",
            "message": "Payment failed due to insufficient funds.",
            "details": "User's card was declined during transaction.",
            "service_name": "microservice_payment"
        },
        {
            "timestamp": datetime.datetime.now().isoformat(),
            "level": "WARN",
            "message": "Inventory running low on item X.",
            "details": "Only 5 units of item X left in stock.",
            "service_name": "microservice_inventory"
        }
    ]
    store_multiple_logs(batch_logs)

