from confluent_kafka import Producer
import json
import time
import random

# Kafka configuration
bootstrap_servers = 'your_kafka_bootstrap_servers'
topic = 'sensor_data'

# Create Kafka producer
producer_config = {'bootstrap.servers': bootstrap_servers}
producer = Producer(producer_config)

# Function to generate sample sensor data
def generate_sensor_data():
    return {
        'sensor_id': random.randint(1, 100),
        'timestamp': int(time.time()),
        'reading': random.uniform(0.0, 100.0)
    }

# Produce messages to Kafka topic
try:
    while True:
        data = generate_sensor_data()
        producer.produce(topic, key=str(data['sensor_id']), value=json.dumps(data))
        producer.flush()
        print(f"Produced message: {data}")
        time.sleep(1)

except KeyboardInterrupt:
    pass

finally:
    producer.flush()
