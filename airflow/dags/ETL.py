from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from io import BytesIO
from io import StringIO
from minio import Minio
import boto3
import csv
import io

# Define the MongoDB connection parameters
mongo_uri = "mongodb://root:root@mongodb:27017/"
database_name = "sales-data"
collection_name = "sales-data"

# minio connection parameters
minio_endpoint = 'http://host.docker.internal:9000'
minio_access_key = "qN8Mrz2x9f8Q41fQhJ6K"
minio_secret_key = "M1nUCZURwyAJa4riEl4WRKIUVoqzVMC1ljFDdGHG"
minio_bucket_name = "silver-layer"
minio_object_key = "processed_data.csv"

categorical_columns = ['Item_Fat_Content', 'Outlet_Size', 'Outlet_Type', 'Outlet_Location_Type']

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

    # Convert extracted data to CSV format
    csv_data = io.StringIO()
    writer = csv.DictWriter(csv_data, fieldnames=extracted_data[0].keys())
    writer.writeheader()
    writer.writerows(extracted_data)

    # Return CSV data as a string
    return csv_data.getvalue()


# Transformation functions
def find_categorical_numerical_columns(df):
    categorical_features = df.select_dtypes(include=['object']).columns
    numerical_features = df.select_dtypes(include=['int64', 'float64']).columns
    return categorical_features, numerical_features

def fill_missing_values(df):
    df['Item_Weight'] = df['Item_Weight'].replace('', np.nan)
    median_weight = df['Item_Weight'].median(skipna=True)
    df['Item_Weight'].fillna(median_weight, inplace=True)
    df['Outlet_Size'] = np.where(df['Outlet_Size'].isna(), df['Outlet_Size'].mode()[0], df['Outlet_Size'])
    return df

def clean_item_fat_content(df):
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace('low fat', 'Low Fat')
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace('LF', 'Low Fat')
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace('reg', 'Regular')
    return df

def label_encode_categorical_features(df, categorical_columns):
    label_encoder = LabelEncoder()
    for feature in categorical_columns:
        df[feature] = label_encoder.fit_transform(df[feature])
    return df


def add_item_identifier_categories_column(df):
    df['Item_Identifier_Categories'] = df['Item_Identifier'].str[:2]
    return df

def one_hot_encode(df, columns):
    return pd.get_dummies(df, columns=columns, drop_first=True)

def drop_column(df, column_name):
    df.drop(labels=[column_name], axis=1, inplace=True)
    return df

def transform_data(csv_data, **kwargs):
    # Convert CSV data to DataFrame
    df = pd.read_csv(StringIO(csv_data))

    # Perform data processing
    categorical_columns, _ = find_categorical_numerical_columns(df)
    df = fill_missing_values(df)
    df = clean_item_fat_content(df)
    df = add_item_identifier_categories_column(df)
    df = label_encode_categorical_features(df, categorical_columns)
    df = one_hot_encode(df, columns=['Item_Type', 'Item_Identifier_Categories', 'Outlet_Identifier'])  # One-hot encode before dropping 'Item_Identifier'
    df = drop_column(df, '_id')  # Drop '_id' column
    df = drop_column(df, 'Item_Identifier')  # Drop 'Item_Identifier' column
    
    # Save transformed data back to CSV
    transformed_csv_data = df.to_csv(index=False)

    # Pass the transformed CSV data to the next task using XCom
    kwargs['ti'].xcom_push(key='transformed_csv_data', value=transformed_csv_data)


# Define a function to save transformed data to Minio
def save_to_minio(transformed_csv_data, minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name, minio_object_key, **kwargs):
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
    dag_id='Extract_Transform_load',
    default_args={
        'owner': 'adithya',
        'start_date': datetime(2024, 4, 24),
        'catchup': False
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

# Define the task to transform data
transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    op_args=[
        "{{ ti.xcom_pull(task_ids='extract_data_task') }}",  # Pass the CSV data fetched from the previous task
    ],
    provide_context=True,
    dag=dag
)

# Define the task to save transformed data to Minio
save_to_minio_task = PythonOperator(
    task_id='save_to_minio_task',
    python_callable=save_to_minio,
    op_args=[
        '{{ task_instance.xcom_pull(task_ids="transform_data_task", key="transformed_csv_data") }}', 
        minio_endpoint, 
        minio_access_key, 
        minio_secret_key, 
        minio_bucket_name, 
        minio_object_key
    ],
    provide_context=True,
    dag=dag
)


# Task dependencies
extract_data_task >> transform_data_task >> save_to_minio_task