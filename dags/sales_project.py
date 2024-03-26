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
    'start_date': datetime(2024, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG with its structure and tasks
dag = DAG('Data_store', 
          default_args=default_args, 
          description='sales_project_data',
          schedule_interval=timedelta(days=1))

# Define tasks
def data_to_minio():
    data=pd.read_csv(file_link)
     # Convert DataFrame to CSV string
    print(data.head(5))
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    
    # Reset the buffer position to the beginning
    csv_buffer.seek(0)
    s3 = boto3.client('s3', 
                      endpoint_url='http://host.docker.internal:9000',
                      aws_access_key_id='s9W8rnDpm0gqigSehI9x',
                      aws_secret_access_key='6GlbBYhBz4fF5UTBcTvCMJSE0DQrkBHtb5pqhaPZ')
    
    
    bucket_name = 'sales-data'
    object_name = 'Train.csv', 'Test.csv'
    
    try:
        # Upload file to MinIO
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        print(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}'")
    except Exception as e:
        print(e)
        



  
raw_data = PythonOperator(task_id='load_raw_data', python_callable=data_to_minio, dag=dag)

raw_data