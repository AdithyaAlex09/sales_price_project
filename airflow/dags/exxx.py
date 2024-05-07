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
from sklearn.linear_model import LinearRegression
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
linear_model_object_name = 'linear_model.pkl'
lasso_model_object_name = 'lasso_model.pkl'

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
    


def trained_model(**kwargs):
    # Extract context (dataframe) from kwargs
    task_instance = kwargs['ti']
    df = task_instance.xcom_pull(task_ids='extract_load_task')
    
    # Define features and target
    X = df.drop(columns=[TARGET_COLUMN])
    y = df[TARGET_COLUMN]
    
    # Split data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Linear Regression model
    linear_regression_model = LinearRegression()
    linear_regression_model.fit(x_train, y_train)

    # Initialize and train the Lasso Regression model
    lasso_regression_model = Lasso(alpha=0.1, max_iter=1000)
    lasso_regression_model.fit(x_train, y_train)

    # Calculate evaluation metrics for Linear Regression
    linear_train_score = linear_regression_model.score(x_train, y_train)
    linear_test_score = linear_regression_model.score(x_test, y_test)
    linear_y_train_pred = linear_regression_model.predict(x_train)
    linear_y_test_pred = linear_regression_model.predict(x_test)
    linear_train_mae = mean_absolute_error(y_train, linear_y_train_pred)
    linear_test_mae = mean_absolute_error(y_test, linear_y_test_pred)
    linear_train_mse = mean_squared_error(y_train, linear_y_train_pred)
    linear_test_mse = mean_squared_error(y_test, linear_y_test_pred)
    linear_train_rmse = np.sqrt(linear_train_mse)
    linear_test_rmse = np.sqrt(linear_test_mse)
    linear_train_r2 = r2_score(y_train, linear_y_train_pred)
    linear_test_r2 = r2_score(y_test, linear_y_test_pred)

    # Print evaluation metrics for Linear Regression
    logging.info("Linear Regression Train Score: %f", linear_train_score)
    logging.info("Linear Regression Test Score: %f", linear_test_score)
    logging.info("===============================================")
    logging.info("Linear Train MAE: %f", linear_train_mae)
    logging.info("Linear Train MSE: %f", linear_train_mse)
    logging.info("Linear Train RMSE: %f", linear_train_rmse)
    logging.info("Linear Train R2 Score: %f", linear_train_r2)
    logging.info("================================================")
    logging.info("Linear Test MAE: %f", linear_test_mae)
    logging.info("Linear Test MSE: %f", linear_test_mse)
    logging.info("Linear Test RMSE: %f", linear_test_rmse)
    logging.info("Linear Test R2 Score: %f", linear_test_r2)

    # Calculate evaluation metrics for Lasso Regression
    lasso_train_score = lasso_regression_model.score(x_train, y_train)
    lasso_test_score = lasso_regression_model.score(x_test, y_test)
    lasso_y_train_pred = lasso_regression_model.predict(x_train)
    lasso_y_test_pred = lasso_regression_model.predict(x_test)
    lasso_train_mae = mean_absolute_error(y_train, lasso_y_train_pred)
    lasso_test_mae = mean_absolute_error(y_test, lasso_y_test_pred)
    lasso_train_mse = mean_squared_error(y_train, lasso_y_train_pred)
    lasso_test_mse = mean_squared_error(y_test, lasso_y_test_pred)
    lasso_train_rmse = np.sqrt(lasso_train_mse)
    lasso_test_rmse = np.sqrt(lasso_test_mse)
    lasso_train_r2 = r2_score(y_train, lasso_y_train_pred)
    lasso_test_r2 = r2_score(y_test, lasso_y_test_pred)

    # Print evaluation metrics for Lasso Regression
    logging.info("Lasso Regression Train Score: %f", lasso_train_score)
    logging.info("Lasso Regression Test Score: %f", lasso_test_score)
    logging.info("===============================================")
    logging.info("Lasso Train MAE: %f", lasso_train_mae)
    logging.info("Lasso Train MSE: %f", lasso_train_mse)
    logging.info("Lasso Train RMSE: %f", lasso_train_rmse)
    logging.info("Lasso Train R2 Score: %f", lasso_train_r2)
    logging.info("================================================")
    logging.info("Lasso Test MAE: %f", lasso_test_mae)
    logging.info("Lasso Test MSE: %f", lasso_test_mse)
    logging.info("Lasso Test RMSE: %f", lasso_test_rmse)
    logging.info("Lasso Test R2 Score: %f", lasso_test_r2)
    
    # Serialize the model objects
    linear_model_dict = {
        "coefficients": linear_regression_model.coef_.tolist(),
        "intercept": linear_regression_model.intercept_,
        # Add any other relevant information about the model
    }

    # Convert Lasso coefficients to list of floats for serialization
    lasso_coefficients = lasso_regression_model.coef_.tolist()
    lasso_intercept = lasso_regression_model.intercept_

    lasso_model_dict = {
        "coefficients": lasso_coefficients,
        "intercept": lasso_intercept,
        # Add any other relevant information about the model
    }

    # Push the serialized model dictionaries to XCom
    task_instance.xcom_push(key='linear_regression_model', value=json.dumps(linear_model_dict))
    task_instance.xcom_push(key='lasso_regression_model', value=json.dumps(lasso_model_dict))

def save_models(bucket_name, object_name, **kwargs):
    try:
        # Retrieve the serialized model from XCom
        linear_model_dict_str = kwargs['ti'].xcom_pull(task_ids='train_model_task', key='linear_regression_model')
        lasso_model_dict_str = kwargs['ti'].xcom_pull(task_ids='train_model_task', key='lasso_regression_model')
        
        linear_model_dict = json.loads(linear_model_dict_str)
        lasso_model_dict = json.loads(lasso_model_dict_str)
        
        # Save the model data to MinIO
        with BytesIO() as f:
            json.dump(linear_model_dict, f)
            f.seek(0)
            s3.put_object(Bucket=bucket_name, Key=f"{object_name}/linear_model.json", Body=f)
        
        with BytesIO() as f:
            json.dump(lasso_model_dict, f)
            f.seek(0)
            s3.put_object(Bucket=bucket_name, Key=f"{object_name}/lasso_model.json", Body=f)
        
        logging.info(f"Models saved successfully to MinIO bucket: s3://{bucket_name}/{object_name}")
    except Exception as e:
        logging.error(f"Error saving models to MinIO: {e}")

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
    'minio_extract_load_train_dag-0.1',
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
    python_callable=trained_model,
    provide_context=True,
    dag=dag,
)

# Define the task to save the linear regression model to MinIO bucket
save_linear_regression_task = PythonOperator(
    task_id='save_linear_regression_task',
    python_callable=save_models,
    op_kwargs={'bucket_name': model_bucket_name, 'object_name': linear_model_object_name},
    provide_context=True,  # Pass context (trained model) from train_model_task to save_models_task
    dag=dag,
)

# Define the task to save the lasso regression model to MinIO bucket
save_lasso_regression_task = PythonOperator(
    task_id='save_lasso_regression_task',
    python_callable=save_models,
    op_kwargs={'bucket_name': model_bucket_name, 'object_name': lasso_model_object_name},
    provide_context=True,  # Pass context (trained model) from train_model_task to save_models_task
    dag=dag,
)

# Set task dependencies
extract_load_task >> train_model_task
train_model_task >> save_linear_regression_task
train_model_task >> save_lasso_regression_task

