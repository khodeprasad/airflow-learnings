
# cross_task_communication.py
import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def increment_by_1(counter):
    print("Count {counter}!".format(counter=counter))

    return counter + 1


def multiply_by_100(counter):
    print("Count {counter}!".format(counter=counter))

    return counter * 100

with DAG(
    dag_id = 'cross_task_communication',
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


# Goto browser and refresh
# Click on the "cross_task_communication"
# Toggle the ON switch click "Auto-refresh" if not selected

# Goto "Graph View" and show the Graph
# Trigger the run and show the completion

# Go to the "Grid" view

# Click on each task and click on XCom

# The values should be 101 and 900

-------------------------------------------------------------------------------


# cross_task_communication_02.py

import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def increment_by_1(value):
    print("Value {value}!".format(value=value))

    return value + 1

def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids='increment_by_1')

    print("Value {value}!".format(value=value))

    return value * 100

def subtract_9(ti):
    value = ti.xcom_pull(task_ids='multiply_by_100')

    print("Value {value}!".format(value=value))

    return value - 9

def print_value(ti):
    value = ti.xcom_pull(task_ids='subtract_9')

    print("Value {value}!".format(value=value))


with DAG(
    dag_id = 'cross_task_communication',
    description = 'Cross-task communication with XCom',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['xcom', 'python']
) as dag:
    increment_by_1 = PythonOperator(
        task_id = 'increment_by_1',
        python_callable = increment_by_1,
        op_kwargs={'value': 1}
    )

    multiply_by_100 = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable = multiply_by_100
    )

    subtract_9 = PythonOperator(
        task_id = 'subtract_9',
        python_callable = subtract_9
    )

    print_value = PythonOperator(
        task_id = 'print_value',
        python_callable = print_value
    )


increment_by_1 >> multiply_by_100 >> subtract_9 >> print_value



# Go to the cross_task_communication DAG

# Show the Graph view

# Trigger a run

# Once the run is complete go to the Grid view

# Click on each task (IMPORTANT) do this for all tasks

# Show the Logs and then show the XCom value returned by the task




































