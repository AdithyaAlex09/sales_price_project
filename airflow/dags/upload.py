from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def upload_to_minio(local_file_path, object_name, bucket_name, replace=False):
    hook = S3Hook(aws_conn_id='minio-connection')

    if not replace:
        # Check if the object exists
        try:
            hook.get_key(key=object_name, bucket_name=bucket_name)
            # Object exists, no need to upload again
            return
        except:
            # Object does not exist, proceed with upload
            pass

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
        op_kwargs={
            'local_file_path': 'D:\sales_price_project\src\preprocessed_data\preprocessed_test.csv',
            'object_name': 'preprocessed_test.csv',
            'bucket_name': 'silver-data',
            'replace': False  # Modify this according to your requirement
        }
    )

    # Set task dependencies (if any)
    upload_task


