import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator

path_add="/opt/airflow/"
sys.path.append(path_add)


# Define the default arguments for the DAG
default_args = {
    'owner': 'Adithya',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG object
dag = DAG(
    'upload_to_minio_dag',
    default_args=default_args,
    description='A simple DAG to upload data to MinIO',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

# Define the S3CopyObjectOperator to upload data to MinIO
upload_to_minio_task = S3CopyObjectOperator(
    task_id='upload_to_minio',
    source_bucket_key='source_bucket/key',
    dest_bucket_key='dest_bucket/key',
    dest_conn_id='minio_conn_id',
    replace=True,  # Replace the destination object if it already exists
    dag=dag,
)

# Set task dependencies
upload_to_minio_task
