from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator

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
    dag_id='dag-test-spark-with-mail', 
    default_args=default_args, 
    schedule_interval='*/30 * * * *', 
    catchup=False,
) as dag:
    run_spark_app = SparkSubmitOperator(
        task_id='run_spark_app_n_mail',
        application='./airflow/dags/actual-job.py',
        deploy_mode='client',
        name='Airflow Spark Python File Run With Mail',
        conn_id='spark_de_client',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='4g',
        env_vars={
            'PYSPARK_PYTHON': '/home/prasadkhode/miniconda3/envs/airflow/bin/python',
            'PYSPARK_DRIVER_PYTHON': '/home/prasadkhode/miniconda3/envs/airflow/bin/python',
        },
    )

    send_email_success = EmailOperator(
        task_id='send_email_success',
        to='test@test.com',
        subject='Spark Job Successful',
        html_content='<p>The Spark job ran successfully.</p>',
        trigger_rule='all_success',
    )

    send_email_failure = EmailOperator(
        task_id='send_email_failure',
        to='test@test.com',
        subject='Spark Job Failed',
        html_content='<p>The Spark job failed.</p>',
        trigger_rule='one_failed',
    )

    run_spark_app >> [send_email_success, send_email_failure]
