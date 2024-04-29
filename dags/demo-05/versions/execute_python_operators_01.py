from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def print_function():
    print("The simplest possible Python operator!")

with DAG(
    dag_id = 'd05_execute_python_operators_01',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['simple', 'python']
) as dag:
    task = PythonOperator(
        task_id = 'python_task',
        python_callable = print_function
    )

task
