from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'regression_metrics_dag',
    default_args=default_args,
    description='A DAG to run regression metrics',
    schedule_interval='@daily',
)

with dag:
    load_data_task = PythonOperator(
        task_id='load_dataset',
        python_callable=load_dataset,
    )

    split_data_task = PythonOperator(
        task_id='split_dataset',
        python_callable=split_dataset,
        provide_context=True,
    )

    run_metrics_task = PythonOperator(
        task_id='run_regression_metrics',
        python_callable=run_regression_metrics,
        provide_context=True,
    )

    load_data_task >> split_data_task >> run_metrics_task


