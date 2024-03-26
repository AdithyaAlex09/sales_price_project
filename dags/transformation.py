from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the function to run the transformation
def run_transformation():
    # Replace this with your transformation logic
    # For example, you can call a Python script or function that performs the transformation
    print("Running transformation...")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG object
dag = DAG(
    'transformation_dag',
    default_args=default_args,
    description='A simple DAG to run a transformation file',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

# Define the PythonOperator to run the transformation
run_transformation_task = PythonOperator(
    task_id='run_transformation',
    python_callable=run_transformation,
    dag=dag,
)

# Set task dependencies
run_transformation_task
