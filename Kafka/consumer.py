from kafka import KafkaConsumer
import json

# Kafka consumer configuration
consumer = KafkaConsumer('test_topic',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         group_id='my-group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Consume messages from the 'test_topic' topic
for message in consumer:
    print("Received:", message.value)
