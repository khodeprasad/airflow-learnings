# https://airflow.apache.org/docs/apache-airflow/1.10.4/howto/operator/python.html

# execute_python_operators_01.py ()

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
    dag_id = 'execute_python_operators',
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
    

# Click on the main Airflow page
# Click on the "execute_python_operators"

# Goto "Graph View" observe there is one task which is a PythonOperator

# Toggle the ON switch click "Auto-refresh" if not selected


# Now click on the "task" -> click on "Log"
# Should be able to see the simplest possible Python operator!


----------------------------------------------------------------------------


# execute_python_operators_02.py
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
    print("TASK C executed!")

def task_d():
    print("TASK D executed!")            

with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['dependencies', 'python']
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

taskA >> [taskB, taskC]
[taskB, taskC] >> taskD



# Refresh the browser and click "execute_python_operators"

# Go to the "Graph" view

# Explicitly run the DAG - show the run on the Graph view


# Now switch to the Grid view to see the logs

# Now click on the "taskA" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed TASK A is executed!

# Now click on the "taskB" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed TASK B is executed!

# Now click on the "taskC" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed TASK C is executed!

# Now click on the "taskD" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed TASK D is executed!


----------------------------------------------------------------------------


# execute_python_operators_03.py

mport time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def greet_hello(name):
    print("Hello, {name}!".format(name=name))

def greet_hello_with_city(name, city):
    print("Hello, {name} from {city}".format(name=name, city=city))


with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs with parameters',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['parameters', 'python']
) as dag:
    taskA = PythonOperator(
        task_id = 'greet_hello',
        python_callable = greet_hello,
        op_kwargs={'name': 'Desmond'}
    )

    taskB = PythonOperator(
        task_id = 'greet_hello_with_city',
        python_callable = greet_hello_with_city,
        op_kwargs={'name': 'Louise', 'city': 'Seattle'}
    )


taskA >> taskB





















