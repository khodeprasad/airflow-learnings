import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook

# Define the DAG
default_args = {
    'owner': 'Prasad Khode',
    'start_date': days_ago(1),
    'retries': 1,
}

user_home = "/e2deepde/pkhode/data_aggregation"
tenant_id = "343"
till_date = "2024-06-04"

dag = DAG(
    'download_s3_file',
    default_args=default_args,
    description='Download a file from S3 and save it to a local path',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def download_file_from_s3(bucket_name, s3_key, local_path, aws_conn_id='aws_default'):
    print('bucket_name', bucket_name)
    print('s3_key', s3_key)
    print('local_path', local_path)

    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3.get_conn()
    s3_client.download_file(bucket_name, s3_key, local_path)
    print(f"Downloaded {s3_key} from bucket {bucket_name} to {local_path}")


download_task = PythonOperator(
    task_id='download_file',
    python_callable=download_file_from_s3,
    op_kwargs={
        'bucket_name': 'bucket_name',
        's3_key': f"json_data/{tenant_id}/tracker/{till_date.replace('-', '/')}/data.json",
        'local_path': f"{user_home}/tracker/{tenant_id}/{till_date.replace('-', '/')}/data.json",
    },
    dag=dag,
)

download_task
