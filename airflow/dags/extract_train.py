import numpy as np 
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from minio import Minio
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Define MinIO connection parameters
MINIO_ENDPOINT = 'http://host.docker.internal:9000'
MINIO_ACCESS_KEY = "DJYHVYXBiu7G47ZX4bUi"
MINIO_SECRET_KEY = "m3q9gXc7RZVy3n7Q4WJa6lPsPPzpxAf2Xobe9rbz"
MINIO_SECURE = True  # Set to False if MinIO is running without SSL

# Define your MinIO bucket and file information
MINIO_BUCKET_NAME = "silver-layer"
MINIO_OBJECT_NAME = "processed_data.csv"

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

# Function to extract and load data from MinIO
def extract_and_load_data():
    try:
        # Download file from MinIO
        data = minio_client.get_object(MINIO_BUCKET_NAME, MINIO_OBJECT_NAME)
        # Read the data (assuming it's CSV for this example)
        df = pd.read_csv(data)
        # Further processing, feature engineering, etc.
        return df
    except Exception as err:
        print(f"Error: {err}")
    

# Function to train machine learning model
def train_model(**kwargs):
    # Extract context (dataframe) from kwargs
    task_instance = kwargs['ti']
    df = task_instance.xcom_pull(task_ids='extract_load_task')
    
    # Define features and target
    X = df.drop(columns=['target_column'])
    y = df['target_column']
    
    # Split data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Linear Regression model
    linear_regression_model = LinearRegression()
    linear_regression_model.fit(x_train, y_train)

    # Calculate train score
    train_score = linear_regression_model.score(x_train, y_train)
    print("Linear Regression Train Score:", train_score)

    # Calculate test score
    test_score = linear_regression_model.score(x_test, y_test)
    print("Linear Regression Test Score:", test_score)

    # Make predictions
    y_train_pred = linear_regression_model.predict(x_train)
    y_test_pred = linear_regression_model.predict(x_test)

    # Calculate evaluation metrics
    train_mae = mean_absolute_error(y_train, y_train_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    train_mse = mean_squared_error(y_train, y_train_pred)
    test_mse = mean_squared_error(y_test, y_test_pred)
    train_rmse = np.sqrt(train_mse)
    test_rmse = np.sqrt(test_mse)
    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)

    # Print evaluation metrics
    print("Train MAE:", train_mae)
    print("Train MSE:", train_mse)
    print("Train RMSE:", train_rmse)
    print("Train R2 Score:", train_r2)
    print("================================================")
    print("Test MAE:", test_mae)
    print("Test MSE:", test_mse)
    print("Test RMSE:", test_rmse)
    print("Test R2 Score:", test_r2)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'minio_extract_load_train_dag',
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
    python_callable=train_model,
    provide_context=True,  # Pass context (dataframe) from extract_load_task to train_model_task
    dag=dag,
)

# Set task dependencies
extract_load_task >> train_model_task
