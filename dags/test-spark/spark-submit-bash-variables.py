from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

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
    dag_id='dag_test_spark_jar_using_bash_varaibles',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:
    
    spark_task = BashOperator(
        task_id='spark_java',
        bash_command=(
            'spark-submit --master {{ params.master }} --driver-memory 2g --executor-memory 2g --executor-cores 2 --total-executor-cores 4 --class {{ params.class }} {{ params.jar }}'
        ),
        params={
            'master': Variable.get("de_spark_master_url"),
            'class': 'org.apache.spark.examples.SparkPi',
            'jar': '/home/pkhode/spark-examples_2.12-3.4.1.jar'
        }
    )

    spark_task
