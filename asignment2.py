from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer, IntegerSerializer
import json
import time
import random

# Set up Kafka topics
topics = ["stock_prices"]
admin_client = AdminClient({'bootstrap.servers': 'your_bootstrap_servers'})

# Create topics if they don't exist
for topic in topics:
    topic_metadata = admin_client.list_topics(topic=topic).topics
    if topic not in topic_metadata:
        new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])

# Set up Kafka producer
producer_conf = {
    'bootstrap.servers': 'your_bootstrap_servers',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': StringSerializer('utf_8')
}
producer = Producer(producer_conf)

# Set up Kafka consumer for alerting
alert_consumer_conf = {
    'bootstrap.servers': 'your_bootstrap_servers',
    'group.id': 'alert-group',
    'key.deserializer': StringSerializer('utf_8'),
    'value.deserializer': StringSerializer('utf_8')
}
alert_consumer = Consumer(alert_consumer_conf)
alert_consumer.subscribe(topics)

# Set up Kafka consumer for average price calculation
average_consumer_conf = {
    'bootstrap.servers': 'your_bootstrap_servers',
    'group.id': 'average-price-group',
    'key.deserializer': StringSerializer('utf_8'),
    'value.deserializer': StringSerializer('utf_8')
}
average_consumer = Consumer(average_consumer_conf)
average_consumer.subscribe(topics)

# Set up Kafka producer for average prices
average_producer_conf = {
    'bootstrap.servers': 'your_bootstrap_servers',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': StringSerializer('utf_8')
}
average_producer = Producer(average_producer_conf)

# Simulate stock price updates
def generate_stock_price():
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    symbol = random.choice(symbols)
    price = round(random.uniform(100, 2000), 2)
    timestamp = int(time.time())
    return {"symbol": symbol, "price": price, "timestamp": timestamp}

# Produce simulated stock price updates
for _ in range(100):
    stock_price = generate_stock_price()
    producer.produce("stock_prices", key=stock_price["symbol"], value=json.dumps(stock_price))
    producer.flush()

# Define the threshold for significant price changes
threshold_percentage = 5.0

# Define the state store for average prices
average_prices = {}

# Consume and process stock price messages
while True:
    # Consume messages for alerting
    alert_msg = alert_consumer.poll(1.0)
    if alert_msg is None:
        continue
    if alert_msg.error():
        if alert_msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(alert_msg.error())
            break

    # Deserialize and process alert message
    alert_data = json.loads(alert_msg.value())
    symbol = alert_data["symbol"]
    price_change_percentage = alert_data["price_change_percentage"]

    # Trigger alert if the price change exceeds the threshold
    if price_change_percentage > threshold_percentage:
        print(f"Alert: Significant price change for {symbol}. Change percentage: {price_change_percentage}%")

    # Consume messages for average price calculation
    average_msg = average_consumer.poll(1.0)
    if average_msg is None:
        continue
    if average_msg.error():
        if average_msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(average_msg.error())
            break

    # Deserialize and process average price message
    average_data = json.loads(average_msg.value())
    symbol = average_data["symbol"]
    new_price = average_data["price"]

    # Update the average price in the state store
    if symbol in average_prices:
        count, old_average = average_prices[symbol]
        new_average = ((count * old_average) + new_price) / (count + 1)
        average_prices[symbol] = (count + 1, new_average)
    else:
        average_prices[symbol] = (1, new_price)

    # Periodically log or store the current average price
    if int(time.time()) % 60 == 0:
        print(f"Average price for {symbol}: {average_prices[symbol][1]}")

# Close consumers and producers
alert_consumer.close()
average_consumer.close()
producer.flush()
producer.close()
average_producer.flush()
average_producer.close()
