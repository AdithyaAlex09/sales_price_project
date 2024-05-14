from confluent_kafka import Producer, KafkaError
import json

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'host.docker.internal:9092'
}

# Create a KafkaProducer instance
producer = Producer(producer_config)

# Produce messages to the 'test_topic' topic
for i in range(5):
    message = {'message': f'Test message {i}'}
    producer.produce('sales_topic', json.dumps(message).encode('utf-8'))

# Flush the producer to send messages
producer.flush()


