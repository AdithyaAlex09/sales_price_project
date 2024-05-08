import numpy as np 
import pandas as pd
import boto3
from io import BytesIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sklearn.linear_model import LinearRegression
from datetime import datetime
from minio import Minio
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from botocore.exceptions import ClientError
import logging
import json
import joblib

# Define your MinIO bucket and file information
MINIO_BUCKET_NAME = "gold-layer"
PREDICTIONS_OBJECT_NAME = 'predictions.csv'

MODEL_BUCKET_NAME = 'prediction-model'
MODEL_OBJECT_NAME = 'linear_model.pkl'

TEST_DATA_BUCKET_NAME = 'silver-layer'
TEST_DATA_OBJECT_NAME = 'preprocessed_test.csv'


# Initialize Boto3 client
s3 = boto3.client('s3',
                  endpoint_url='http://host.docker.internal:9000',
                  aws_access_key_id="qN8Mrz2x9f8Q41fQhJ6K",
                  aws_secret_access_key="M1nUCZURwyAJa4riEl4WRKIUVoqzVMC1ljFDdGHG",
                  )

def load_model():
    try:
        # Download model file from MinIO
        response = s3.get_object(Bucket=MODEL_BUCKET_NAME, Key=MODEL_OBJECT_NAME)
        model_bytes = response['Body'].read()
        
        # Deserialize the model dictionary from JSON
        model_dict = json.loads(model_bytes)
        
        # Reconstruct the Lasso model
        linear_regression_model = LinearRegression()
        linear_regression_model.coef_ = np.array(model_dict["coefficients"])
        linear_regression_model.intercept_ = model_dict["intercept"]
        
        return linear_regression_model
    except ClientError as e:
        logging.error(f"Error fetching model from MinIO: {e}")
        raise


# Function to load test data from MinIO
def load_test_data():
    try:
        # Download test data file from MinIO
        response = s3.get_object(Bucket=TEST_DATA_BUCKET_NAME, Key=TEST_DATA_OBJECT_NAME)
        # Read the test data (assuming it's CSV for this example)
        test_data = response['Body'].read()
        df = pd.read_csv(BytesIO(test_data))
        return df
    except ClientError as e:
        logging.error(f"Error fetching test data from MinIO: {e}")
        raise


def make_predictions(model, **context):
    try:
        # Deserialize the model
        deserialized_model = joblib.loads(model)
        
        test_data = context['task_instance'].xcom_pull(task_ids='load_test_data_task')
        
        # Extract features from the test data
        X_test = test_data
        
        # Make predictions
        predictions = deserialized_model.predict(X_test)
        
        return predictions
    except Exception as e:
        logging.error(f"Error making predictions: {e}")
        raise


# Function to save predicted data to MinIO
def save_predictions(predictions):
    try:
        # Create DataFrame with predictions
        predictions_df = pd.DataFrame(predictions, columns=['Predictions'])
        
        # Convert DataFrame to CSV
        predictions_csv = predictions_df.to_csv(index=False)
        
        # Save the predictions data to MinIO
        s3.put_object(Bucket=MINIO_BUCKET_NAME, Key=PREDICTIONS_OBJECT_NAME, Body=predictions_csv)
        logging.info(f"Predictions saved successfully to MinIO bucket: s3://{MINIO_BUCKET_NAME}/{PREDICTIONS_OBJECT_NAME}")
    except Exception as e:
        logging.error(f"Error saving predictions to MinIO: {e}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'predict_test_data_dag',
    default_args=default_args,
    description='DAG for predicting using trained model and saving predictions to MinIO',
    schedule_interval=None,
)

# Load the model
model = load_model()
test_data = load_test_data()

# Define PythonOperator to load the trained model
load_model_task = PythonOperator(
    task_id='load_model_task',
    python_callable=load_model,
    dag=dag,
)

# Define PythonOperator to load the test data
load_test_data_task = PythonOperator(
    task_id='load_test_data_task',
    python_callable=load_test_data,
    dag=dag,
)

# Define PythonOperator to make predictions
make_predictions_task = PythonOperator(
    task_id='make_predictions_task',
    python_callable=make_predictions,
    op_kwargs={'model': model, 'test_data': test_data},
    dag=dag,
)

# Define PythonOperator to save predictions
save_predictions_task = PythonOperator(
    task_id='save_predictions_task',
    python_callable=save_predictions,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
load_model_task >> load_test_data_task >> make_predictions_task >> save_predictions_task




