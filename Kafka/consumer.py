from confluent_kafka import Consumer, KafkaError
import json

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'host.docker.internal:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create a KafkaConsumer instance
consumer = Consumer(consumer_config)

# Subscribe to the 'test_topic' topic
consumer.subscribe(['sales_topic'])

try:
    # Consume messages from the 'test_topic' topic
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                continue
            else:
                # Error
                print(msg.error())
                break
        else:
            # Message received successfully
            print("Received:", json.loads(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    # Close the consumer on interruption
    consumer.close()

