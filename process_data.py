from confluent_kafka import Consumer, KafkaError
import json

# Kafka configuration
bootstrap_servers = 'your_kafka_bootstrap_servers'
topic = 'sensor_data'

# Create Kafka consumer
consumer_config = {'bootstrap.servers': bootstrap_servers, 'group.id': 'my_consumer_group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Function to process received sensor data
def process_sensor_data(message):
    try:
        data = json.loads(message.value())
        print(f"Received message: {data}")
        # Implement your data processing logic here

    except json.JSONDecodeError:
        print(f"Error decoding JSON message: {message.value()}")

# Consume messages from Kafka topic
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        process_sensor_data(msg)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
