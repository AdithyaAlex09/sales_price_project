from kafka import KafkaProducer
import json

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Produce messages to the 'test_topic' topic
for i in range(5):
    message = {'message': f'Test message {i}'}
    producer.send('test_topic', value=message)

# Flush the producer to send messages
producer.flush()

# Close the producer
producer.close()
