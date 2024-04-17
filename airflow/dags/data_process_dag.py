from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

# Define the MongoDB connection parameters
mongo_uri = "mongodb://root:root@mongodb:27017/"
database_name = "sales-data"
collection_name = "sales-data"

# Data processing functions

def find_categorical_numerical_columns(df):
    categorical_features = df.select_dtypes(include=['object']).columns
    numerical_features = df.select_dtypes(include=['int64', 'float64']).columns
    return categorical_features, numerical_features

def fill_missing_values(df):
    df['Item_Weight'] = np.where(df['Item_Weight'].isna(), df['Item_Weight'].median(skipna=True), df['Item_Weight'])
    df['Outlet_Size'] = np.where(df['Outlet_Size'].isna(), df['Outlet_Size'].mode()[0], df['Outlet_Size'])
    return df

def clean_item_fat_content(df):
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace(['low fat', 'LF'], 'Low Fat')
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

def main(extracted_data):
    # Convert extracted data to DataFrame
    df = pd.DataFrame(extracted_data)
    
    # Perform data processing
    categorical_columns, _ = find_categorical_numerical_columns(df)
    df = fill_missing_values(df)
    df = clean_item_fat_content(df)
    
    df = label_encode_categorical_features(df, categorical_columns)
    
    df = add_item_identifier_categories_column(df)
    
    columns = ['Item_Type', 'Item_Identifier_Categories', 'Outlet_Identifier']
    df = one_hot_encode(df, columns)
    
    df = drop_column(df, 'Item_Identifier')
    
    # Save processed data to file
    df.to_csv("/opt/airflow/data/processed_data.csv", index=False)

default_args = {
    'owner': 'adithya',
    'start_date': datetime(2024, 4, 17),
    'catchup': False,
}

with DAG(
    dag_id='process_data_from_mongodb',
    default_args=default_args,
    schedule_interval=None
) as dag:
    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data_from_mongodb,
        provide_context=True,
    )
    
    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=main,
        op_kwargs={'extracted_data': '{{ ti.xcom_pull(task_ids="extract_data_task") }}'}
    )

extract_data_task >> process_data_task