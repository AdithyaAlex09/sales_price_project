from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to upload file to MinIO
def upload_to_minio():
    # Specify MinIO connection parameters
    minio_conn_id = 'minio_conn'  # The connection ID defined in Airflow for MinIO
    bucket_name = 'your_bucket_name'
    object_name = 'example_data.csv'
    local_file_path = '/path/to/local/file/example_data.csv'

    # Upload file to MinIO
    hook = S3Hook(minio_conn_id)
    hook.load_file(local_file_path, object_name, bucket_name)

# Define the DAG
with DAG(
    'upload_to_minio_dag',
    default_args=default_args,
    description='A DAG to upload a data file to MinIO',
    schedule_interval=None,  # You can set the schedule interval as needed
) as dag:

    # Define task using PythonOperator
    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    # Set task dependencies (if any)
    upload_task
