import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def task_a():
    print("TASK A executed!")

def task_b():
    time.sleep(5)
    print("TASK B executed!")

def task_c():
    time.sleep(5)
    print("TASK C executed!")

def task_d():
    time.sleep(5)
    print("TASK D executed!")  

def task_e():
    print("TASK E executed!")          


with DAG(
    dag_id = 'dag_with_operators',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['dependencies', 'python', 'operators']
) as dag:

    taskA = PythonOperator(
        task_id = 'taskA',
        python_callable = task_a
    )

    taskB = PythonOperator(
        task_id = 'taskB',
        python_callable = task_b
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = PythonOperator(
        task_id = 'taskD',
        python_callable = task_d
    )

    taskE = PythonOperator(
        task_id = 'taskE',
        python_callable = task_e
    )

taskA >> [taskB, taskC, taskD] >> taskE


