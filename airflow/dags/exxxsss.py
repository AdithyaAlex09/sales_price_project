from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sklearn.preprocessing import LabelEncoder
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import numpy as np
import csv
import io
import boto3
from airflow.models import XCom

# Define the MongoDB connection parameters
mongo_uri = "mongodb://root:root@mongodb:27017/"
database_name = "sales-data"
collection_name = "sales-data"

# Minio connection parameters
minio_endpoint = 'http://host.docker.internal:9000'
minio_access_key = "alOpB8p0cnB22WJqJHdm"
minio_secret_key = "nwTkhvTcgKtO4a22hBrhYyKoq9ZfEd3lTi4DFPkT"
minio_bucket_name = "silver-data"
minio_object_key = "processed_data.csv"

# Define the function to extract data from MongoDB
def extract_data_from_mongodb():
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

    # Convert extracted data to CSV format
    csv_data = io.StringIO()
    writer = csv.DictWriter(csv_data, fieldnames=extracted_data[0].keys())
    writer.writeheader()
    writer.writerows(extracted_data)

    # Return CSV data as a string
    return csv_data.getvalue()



# Define the function to perform data transformations
def transform_data(csv_data, **kwargs):
    # Convert CSV data to DataFrame
    df = pd.read_csv(io.StringIO(csv_data))

    # Perform data processing
    categorical_columns = df.select_dtypes(include=['object']).columns
    numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns

    # Fill missing values
    df['Item_Weight'] = df['Item_Weight'].fillna(df['Item_Weight'].median())
    df['Outlet_Size'] = df['Outlet_Size'].fillna(df['Outlet_Size'].mode()[0])

    # Clean item fat content
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace({'low fat': 'Low Fat', 'LF': 'Low Fat', 'reg': 'Regular'})

    # Add item identifier categories column
    df['Item_Identifier_Categories'] = df['Item_Identifier'].str[:2]

    # One-hot encode categorical features
    df = pd.get_dummies(df, columns=['Item_Type', 'Item_Identifier_Categories', 'Outlet_Identifier'], drop_first=True)

    # Drop unnecessary columns
    df.drop(columns=['Item_Identifier', '_id'], inplace=True)

    # Save transformed data back to CSV
    transformed_csv_data = df.to_csv(index=False)

    # Push the transformed data to XCom for further processing
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='transformed_csv_data', value=transformed_csv_data)

    # Log or print statement for verification
    print("Transformed data successfully pushed to XCom.")


# Define the function to save transformed data to Minio
def save_to_minio(**kwargs):
    task_instance = kwargs['ti']
    transformed_csv_data = task_instance.xcom_pull(task_ids='transform_data_task', key='transformed_csv_data')
    
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name='',  # region is not needed for MinIO
            config=boto3.session.Config(signature_version='s3v4')
        )

        # Upload CSV data as an object to Minio bucket
        s3.put_object(Bucket=minio_bucket_name, Key=minio_object_key, Body=transformed_csv_data.encode('utf-8'))

        print("Transformed data uploaded to Minio successfully.")

    except Exception as e:
        # Log the error and mark the task as failed
        print(f"Error occurred while uploading data to Minio: {str(e)}")
        raise



# Define the DAG
dag = DAG(
    dag_id='mongodb_Transform_load_0.1',
    default_args={
        'owner': 'adithya',
        'start_date': datetime(2024, 4, 22),
        'catchup': False
    },
    schedule_interval=None
)

# Define the tasks
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data_from_mongodb,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    dag=dag
)


save_to_minio_task = PythonOperator(
    task_id='save_to_minio_task',
    python_callable=save_to_minio,
    dag=dag
)

# Define task dependencies
extract_data_task >> transform_data_task >> save_to_minio_task

