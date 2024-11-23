import subprocess
import time

def start_zookeeper(ip_address):
    print("Starting Zookeeper...")
    zookeeper_process = subprocess.Popen(
        ["bin/zookeeper-server-start.sh", "config/zookeeper.properties"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={"ZOOKEEPER_CLIENT_PORT": f"{ip_address}:2181"}
    )
    time.sleep(5)  # Wait for Zookeeper to start
    return zookeeper_process

def start_kafka(ip_address):
    print("Starting Kafka...")
    kafka_process = subprocess.Popen(
        ["bin/kafka-server-start.sh", "config/server.properties"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={"KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{ip_address}:9092"}
    )
    time.sleep(10)  # Wait for Kafka to start
    return kafka_process

def create_kafka_topics(ip_address):
    print("Creating Kafka topics...")
    topics = ["Alerts", "Logs"]
    for topic in topics:
        subprocess.run(
            ["bin/kafka-topics.sh", "--create", "--topic", topic, "--bootstrap-server", f"{ip_address}:9092", "--partitions", "1", "--replication-factor", "1"],
            check=True
        )

def main(ip_address):
    try:
        zookeeper_process = start_zookeeper(ip_address)
        kafka_process = start_kafka(ip_address)
        create_kafka_topics(ip_address)

        print("Kafka and Zookeeper are running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping Kafka and Zookeeper...")
        kafka_process.terminate()
        zookeeper_process.terminate()
        kafka_process.wait()
        zookeeper_process.wait()
        print("Kafka and Zookeeper stopped.")

if __name__ == "__main__":
    ip_address = "YOUR_IP_ADDRESS"  # Replace with your specific IP address
    main(ip_address)