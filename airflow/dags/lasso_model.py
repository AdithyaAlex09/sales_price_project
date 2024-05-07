import numpy as np 
import pandas as pd
import boto3
import joblib
from airflow import DAG
from io import BytesIO
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from minio import Minio
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from botocore.exceptions import ClientError
import logging
import json

# Define your MinIO bucket and file information
MINIO_BUCKET_NAME = "silver-layer"
MINIO_OBJECT_NAME = "processed_data.csv"
TARGET_COLUMN = "Item_Outlet_Sales"

model_bucket_name = 'prediction-model'
model_object_name = 'lasso_model.pkl'

# Initialize Boto3 client
s3 = boto3.client('s3',
                  endpoint_url='http://host.docker.internal:9000',
                  aws_access_key_id="qN8Mrz2x9f8Q41fQhJ6K",
                  aws_secret_access_key="M1nUCZURwyAJa4riEl4WRKIUVoqzVMC1ljFDdGHG",
                  )

# Function to extract and load data from MinIO
def extract_and_load_data():
    try:
        # Download file from MinIO
        response = s3.get_object(Bucket=MINIO_BUCKET_NAME, Key=MINIO_OBJECT_NAME)
        # Read the data (assuming it's CSV for this example)
        data = response['Body'].read()
        df = pd.read_csv(BytesIO(data))
        # Further processing, feature engineering, etc.
        return df
    except ClientError as e:
        logging.error(f"Error fetching data from MinIO: {e}")
        raise
    

# Function to train Lasso machine learning model
def trained_lasso_model(**kwargs):
    # Extract context (dataframe) from kwargs
    task_instance = kwargs['ti']
    df = task_instance.xcom_pull(task_ids='extract_load_task')
    
    # Define features and target
    X = df.drop(columns=[TARGET_COLUMN])
    y = df[TARGET_COLUMN]
    
    # Split data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Lasso Regression model
    lasso_regression_model = Lasso(alpha=0.1, max_iter=1000)
    lasso_regression_model.fit(x_train, y_train)

    # Calculate evaluation metrics
    train_score = lasso_regression_model.score(x_train, y_train)
    test_score = lasso_regression_model.score(x_test, y_test)
    y_train_pred = lasso_regression_model.predict(x_train)
    y_test_pred = lasso_regression_model.predict(x_test)
    train_mae = mean_absolute_error(y_train, y_train_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    train_mse = mean_squared_error(y_train, y_train_pred)
    test_mse = mean_squared_error(y_test, y_test_pred)
    train_rmse = np.sqrt(train_mse)
    test_rmse = np.sqrt(test_mse)
    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)

    # Print evaluation metrics
    logging.info("Lasso Regression Train Score: %f", train_score)
    logging.info("Lasso Regression Test Score: %f", test_score)
    logging.info("===============================================")
    logging.info("Train MAE: %f", train_mae)
    logging.info("Train MSE: %f", train_mse)
    logging.info("Train RMSE: %f", train_rmse)
    logging.info("Train R2 Score: %f", train_r2)
    logging.info("================================================")
    logging.info("Test MAE: %f", test_mae)
    logging.info("Test MSE: %f", test_mse)
    logging.info("Test RMSE: %f", test_rmse)
    logging.info("Test R2 Score: %f", test_r2)
    
    # Serialize the model object
    model_dict = {
        "coefficients": lasso_regression_model.coef_.tolist(),
        "intercept": lasso_regression_model.intercept_,
        # Add any other relevant information about the model
    }
    
    # Log the model dictionary
    logging.info("Model dictionary: %s", model_dict)

    # Push the serialized model dictionary to XCom
    task_instance.xcom_push(key='lasso_regression_model', value=json.dumps(model_dict))
    

# Function to save model to MinIO
def save_model(bucket_name, object_name, **kwargs):
    try:
        # Retrieve the serialized model from XCom
        model_dict_str = kwargs['ti'].xcom_pull(task_ids='train_model_task', key='lasso_regression_model')
        
        # Add logging to check the value of model_dict_str
        logging.info(f"Model dictionary string: {model_dict_str}")
        
        model_dict_bytes = model_dict_str.encode('utf-8')  # Convert JSON string to bytes
        
        # Save the model data to MinIO
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=model_dict_bytes)
        logging.info(f"Model saved successfully to MinIO bucket: s3://{bucket_name}/{object_name}")
    except Exception as e:
        logging.error(f"Error saving model to MinIO: {e}")



# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'minio_extract_load_train-lasso_dag',
    default_args=default_args,
    description='DAG for extracting data from MinIO, loading, and training ML model',
    schedule_interval=None,
)

# Define PythonOperator to execute the extract and load function
extract_load_task = PythonOperator(
    task_id='extract_load_task',
    python_callable=extract_and_load_data,
    dag=dag,
)

# Define PythonOperator to execute the train model function
train_model_task = PythonOperator(
    task_id='train_model_task',
    python_callable=trained_lasso_model,
    provide_context=True,
    dag=dag,
)

# Define the task to save the model to MinIO bucket
save_model_task = PythonOperator(
    task_id='save_model_task',
    python_callable=save_model,
    op_kwargs={'bucket_name': model_bucket_name, 'object_name': model_object_name},
    provide_context=True,  # Pass context (trained model) from train_model_task to save_model_task
    dag=dag,
)

# Set task dependencies
extract_load_task >> train_model_task >> save_model_task
