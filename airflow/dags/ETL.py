from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import boto3
from botocore.exceptions import NoCredentialsError
from pymongo import MongoClient

# DAG parameters
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 15),
}

# MongoDB connection parameters
mongo_uri = "mongodb://root:root@mongodb:27017/"
database_name = "sales-data"
collection_name = "sales-data"



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

def transform_data(**kwargs):
    # Retrieve extracted data from XCom
    extracted_data = kwargs['ti'].xcom_pull(task_ids='extract_data_from_mongodb')

    df = pd.DataFrame(extracted_data)
    
    def find_categorical_numerical_columns(df):
        categorical_features = df.select_dtypes(include=['object']).columns
        numerical_features = df.select_dtypes(include=['int64', 'float64']).columns
        return categorical_features, numerical_features

    def fill_missing_values(df):
      if 'Item_Weight' in df.columns:
        df['Item_Weight'] = np.where(df['Item_Weight'].isna(), df['Item_Weight'].median(skipna=True), df['Item_Weight'])
      if 'Outlet_Size' in df.columns:
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
    
    categorical_features, numerical_features = find_categorical_numerical_columns(df)
    print("Categorical columns:", categorical_features)
    print("Numerical columns:", numerical_features)

    df = fill_missing_values(df)
    df = clean_item_fat_content(df)
    
    categorical_columns = ['Item_Fat_Content', 'Outlet_Size', 'Outlet_Type', 'Outlet_Location_Type']
    df = label_encode_categorical_features(df, categorical_columns)
    
    df = add_item_identifier_categories_column(df)
    
    train_columns = ['Item_Type', 'Item_Identifier_Categories', 'Outlet_Identifier']
    df = one_hot_encode(df, train_columns)
    
    df = drop_column(df, 'Item_Identifier')
    
    # Pass transformed data to the next task
    kwargs['ti'].xcom_push(key='transformed_data', value=df)


def save_to_s3(**kwargs):
    # AWS credentials and S3 bucket name
    AWS_ACCESS_KEY = 'Zlri41bGsNTg63Qc3qFF'
    AWS_SECRET_KEY = 'Yw1lJ9apR9efLAyMhfcJyErM0YpcCRnIevZFJoEq'
    S3_BUCKET_NAME = 'silver-data'
    # Retrieve transformed data from XCom
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,endpoint_url='http://host.docker.internal:9000')
    try:
        # Convert DataFrame to JSON string
        json_buffer = transformed_data.to_json(orient='records')
        # Upload JSON string to S3
        file_name = 'preprocessed_data.json'
        s3.put_object(Body=json_buffer.encode('utf-8'), Bucket=S3_BUCKET_NAME, Key=file_name)
        print(f"File '{file_name}' uploaded to S3 bucket '{S3_BUCKET_NAME}'")
    except NoCredentialsError:
        print("AWS credentials not available. Unable to upload file to S3.")



# Define the DAG
dag = DAG(
    dag_id='ETL_DAG',
    default_args=default_args,
    description='Data Preprocessing DAG',
    schedule_interval=None,
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data_from_mongodb',
    python_callable=extract_data_from_mongodb,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Pass the output of the extract task to the transform task
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_to_s3',
    python_callable=save_to_s3,
    provide_context=True,  # Pass the output of the transform task to the save task
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> save_task


