from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'Prasad Khode',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_test_spark_connection', 
    default_args=default_args, 
    schedule_interval='*/30 * * * *', 
    catchup=False,
) as dag:
    run_spark_app = SparkSubmitOperator(
        task_id='dag_test_spark_connection_task',
        application='./airflow/dags/actual-job.py',
        deploy_mode='client',
        name='Airflow Spark Python File Run',
        conn_id='spark_de_client',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='4g',
        env_vars={
            'PYSPARK_PYTHON': '/home/prasadkhode/miniconda3/envs/airflow/bin/python',
            'PYSPARK_DRIVER_PYTHON': '/home/prasadkhode/miniconda3/envs/airflow/bin/python',
        },
    )
