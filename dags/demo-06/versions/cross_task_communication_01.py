import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'PK'
}

def increment_by_1(counter):
    print("Count {counter}!".format(counter=counter))

    return counter + 1


def multiply_by_100(counter):
    print("Count {counter}!".format(counter=counter))

    return counter * 100


# def subtract_9(counter):
#     print("Count {counter}!".format(counter=counter))

#     return counter - 9

# def print(counter):
#     print("Count {counter}!".format(counter=counter))

#     return counter - 9

with DAG(
    dag_id = 'd06_cross_task_communication_01',
    description = 'Cross-task communication with XCom',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['xcom', 'python']
) as dag:
    taskA = PythonOperator(
        task_id = 'increment_by_1',
        python_callable = increment_by_1,
        op_kwargs={'counter': 100}
    )

    taskB = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable = multiply_by_100,
        op_kwargs={'counter': 9}
    )


taskA >> taskB



