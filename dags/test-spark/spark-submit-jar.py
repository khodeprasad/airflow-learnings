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
    dag_id='dag_test_spark_jar',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/home/pkhode/spark-examples_2.12-3.4.1.jar',  # Path to your JAR file
        deploy_mode='client',  # Change to 'client' if you want to run in client mode
        name='Airflow Spark Jar Submit',
        conn_id='spark_de_client',  # Spark connection ID configured in Airflow
        application_args=['100'],  # Argument for SparkPi (number of slices)
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='4g',
        java_class='org.apache.spark.examples.SparkPi',  # Specify the main class
        # jars='/path/to/extra.jar',  # Path to additional JARs if required
        # packages='com.example:example-package:1.0.0',  # Additional packages if needed
        # driver_class_path='/path/to/driver-classpath',  # Driver class path if needed
        # env_vars={
        #     'PYSPARK_PYTHON': '/path/to/python3.10',  # Ensure the correct Python version
        #     'PYSPARK_DRIVER_PYTHON': '/path/to/python3.10',  # Ensure both are the same
        # },
        verbose=True  # Enable verbose logging
    )

    run_spark_job
    