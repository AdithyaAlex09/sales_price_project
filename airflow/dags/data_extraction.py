from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import json

# Define the MongoDB connection parameters
mongo_uri = "mongodb://root:root@mongodb:27017/"
database_name = "sales-data"
collection_name = "sales-data"

# Define the function to extract data from MongoDB
def extract_data_from_mongodb(**kwargs):
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Query the collection to extract data
    cursor = collection.find({})

    # Process the extracted data
    extracted_data = []
    for document in cursor:
        # Serialize ObjectId to string
        document['_id'] = str(document['_id'])
        extracted_data.append(document)

    # Close the MongoDB connection
    client.close()

    # Pass extracted data to the next task without printing it
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

# Define the DAG
dag = DAG(
    dag_id='extract_data_from_mongodb',
    default_args={
        'owner': 'adithya',
        'start_date': datetime(2024, 4, 16),
        'catchup': False,
        'start_date': datetime(2024, 4, 17),
    },
    schedule_interval=None
)

# Define the task to extract data from MongoDB
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data_from_mongodb,
    provide_context=True,
    dag=dag
)

# Define a task to save the extracted data locally
def save_locally(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(task_ids='extract_data_task', key='extracted_data')
    # Save the extracted data to a local file
    with open("/opt/airflow/data/extracted_data.json", "w") as f:
        json.dump(extracted_data, f)

# Save the extracted data to a local file
save_data_task = PythonOperator(
    task_id='save_data_locally',
    python_callable=save_locally,
    provide_context=True,
    dag=dag
)

# Define the task dependencies
extract_data_task >> save_data_task
