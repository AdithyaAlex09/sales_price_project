import sys
import os
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from io import StringIO
from io import BytesIO
import boto3

path_add="/opt/airflow/"
sys.path.append(path_add)


# enviornment varabiles 
file_link=['/opt/airflow/data/Train.csv' , '/opt/airflow/data/Test.csv']

default_args = {
    'owner': 'ADITHYA',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'catchup':False 
}

# Define the DAG with its structure and tasks
dag = DAG('Data_store', 
          default_args=default_args, 
          description='sales_project_data',
          schedule_interval=timedelta(days=1))

def data_to_minio():
    # Iterate over each file path
    for file_path in file_link:
        # Read data from CSV file
        data = pd.read_csv(file_path)
        print(data.head(5))
        
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        
        # Reset the buffer position to the beginning
        csv_buffer.seek(0)
        
        # Upload file to MinIO
        s3 = boto3.client('s3', 
                          endpoint_url='http://host.docker.internal:9000',
                          aws_access_key_id='AymNMGGtvD3IP7nTTHfH',
                          aws_secret_access_key='bIEUvRpLyU5o0eBe018uEqNDSQjr0fHOdRDHc2QK')
        
        bucket_name = 'bronze-data'
        object_name = os.path.basename(file_path)
        
        try:
            s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
            print(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}'")
        except Exception as e:
            print(e)

# Define tasks
raw_data = PythonOperator(task_id='load_raw_data', python_callable=data_to_minio, dag=dag)
raw_data