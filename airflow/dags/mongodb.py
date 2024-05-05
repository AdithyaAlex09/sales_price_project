from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

default_args = {
    'owner': 'adithya',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def upload_to_mongodb():
    # Connect to MongoDB with authentication
    client = MongoClient('mongodb://root:root@mongodb:27017/')
    db = client['sales-data']
    collection = db['sales-data']
    
    # Read data from CSV
    df = pd.read_json('/opt/airflow/data/Traindata.json')  # Path to your CSV file
    
    # Convert DataFrame to dictionary records
    records = df.to_dict(orient='records')
    
    # Insert records into MongoDB collection
    collection.insert_many(records)

dag = DAG(
    dag_id='upload_to_mongodb',
    default_args=default_args,
    description='Upload data to MongoDB',
    schedule_interval=None,
    start_date=datetime(2024, 5, 4),
    catchup=False
)

upload_to_mongodb_task = PythonOperator(
    task_id='upload_to_mongodb',
    python_callable=upload_to_mongodb,
    dag=dag
)

upload_to_mongodb_task

