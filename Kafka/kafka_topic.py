from confluent_kafka.admin import AdminClient, NewTopic
import logging

# Kafka broker configuration
bootstrap_servers = 'host.docker.internal:9092'

def create_topic(topic_name, partitions=1, replication_factor=1):
    try:
        # Initialize Kafka admin client
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Create a new topic
        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)

        # Create the topic
        admin_client.create_topics([new_topic])

        print(f"Topic '{topic_name}' created successfully with {partitions} partitions and replication factor {replication_factor}")
    except Exception as e:
        logging.error(f"Error creating topic '{topic_name}': {e}")

if __name__ == "__main__":
    # Define the topic name, partitions, and replication factor
    topic_name = 'sales_topic'
    partitions = 1
    replication_factor = 1

    # Create the topic
    create_topic(topic_name, partitions, replication_factor)

