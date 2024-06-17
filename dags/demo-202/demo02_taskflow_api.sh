
--------------------------------------------------------------------------------
# Version 01
--------------------------------------------------------------------------------

# Please make sure that you start with a clean airflow directory and installation

# You should not have any of the extra folders that we have created in the past

# Make sure the only folders you have are

dags/
logs/

# Open up VS Code and show the airflow folders

# Show the dags/ folder under airflow/

# Create a new file under the dags/ folder

# Call it dag_with_operators.py


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


# Switch over to the Airflow UI

# Click through to the DAG in the GridView and show the tasks on the left

# Click on the GraphView and show the DAG (make sure you zoom out)

# Enable the DAG and enable Auto-refresh

# The DAG should run through fine

# Click on Task E and show the logs - we should see "Task E executed"

# Everything has worked just fine


--------------------------------------------------------------------------------
# Version 02
--------------------------------------------------------------------------------

############ Notes ------------------------

# New in version 2.0.

# If you write most of your DAGs using plain Python code rather than Operators, then the TaskFlow API will make it much easier to author clean DAGs without extra boilerplate, all using the @task decorator.

# The TaskFlow API was introduced in Airflow version 2.0 as a new way to define workflows using a more Pythonic and intuitive syntax. It aims to simplify the process of creating complex workflows by providing a higher-level abstraction compared to the traditional DAG-based approach.

# The TaskFlow API introduces a set of new constructs and decorators for defining tasks and dependencies.

# In this data pipeline, tasks are created based on Python functions using the @task decorator as shown below. The function name acts as a unique identifier for the task.
############ Notes ------------------------


# dag_with_taskflow.py


import time
from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner': 'loonycorn'
}

@dag(dag_id='dag_with_taskflow',
     description = 'DAG using the TaskFlow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once',
     tags = ['dependencies', 'python', 'taskflow_api'])
def dag_with_taskflow_api():
    
    @task
    def task_a():
        print("TASK A executed!")
    
    @task
    def task_b():
        time.sleep(5)
        print("TASK B executed!")
    
    @task
    def task_c():
        time.sleep(5)
        print("TASK C executed!")
    
    @task
    def task_d():
        time.sleep(5)
        print("TASK D executed!")
    
    @task
    def task_e():
        print("TASK E executed!")
    
    task_a() >> [task_b(), task_c(), task_d()] >> task_e()


dag_with_taskflow_api()




# Switch over to the Airflow UI

# Click through to the DAG in the GridView and show the tasks on the left

# Click on the GraphView and show the DAG (make sure you zoom out)

# Enable the DAG and enable Auto-refresh

# The DAG should run through fine

# Click on Task E and show the logs - we should see "Task E executed"

# Everything has worked just fine



--------------------------------------------------------------------------------
# Version 03
--------------------------------------------------------------------------------

# Passing data between tasks using operators

############ Notes ------------------------

# The json library is part of the Python standard library, which means it is included in the Python core distribution. The json module provides functionality for encoding and decoding JSON (JavaScript Object Notation) data. It allows you to serialize Python objects into JSON strings and deserialize JSON strings into Python objects.

############ Notes ------------------------


# passing_data_with_operators.py


import time

import json

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'loonycorn'
}

def get_order_prices(**kwargs):
    ti = kwargs["ti"]

    order_price_data = {
        'o1': 234.45, 
        'o2': 10.00, 
        'o3': 34.77, 
        'o4': 45.66, 
        'o5': 399
    }

    order_price_data_string = json.dumps(order_price_data)

    ti.xcom_push('order_price_data', order_price_data_string)


def compute_sum(**kwargs):
    ti = kwargs["ti"]

    order_price_data_string = ti.xcom_pull(
        task_ids='get_order_prices', key='order_price_data')

    print(order_price_data_string)

    order_price_data = json.loads(order_price_data_string)

    total = 0
    for order in order_price_data:
        total += order_price_data[order] 

    ti.xcom_push('total_price', total)


def compute_average(**kwargs):
    ti = kwargs["ti"]

    order_price_data_string = ti.xcom_pull(
        task_ids='get_order_prices', key='order_price_data')

    print(order_price_data_string)

    order_price_data = json.loads(order_price_data_string)

    total = 0
    count = 0
    for order in order_price_data:
        total += order_price_data[order] 
        count += 1

    average = total / count

    ti.xcom_push('average_price', average)


def display_result(**kwargs):
    ti = kwargs["ti"]

    total = ti.xcom_pull(
        task_ids='compute_sum', key='total_price')
    average = ti.xcom_pull(
        task_ids='compute_average', key='average_price')


    print("Total price of goods {total}".format(total=total))
    print("Average price of goods {average}".format(average=average))


with DAG(
    dag_id = 'cross_task_communication_operators',
    description = 'XCom with operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['xcom', 'python', 'operators']
) as dag:
    get_order_prices = PythonOperator(
        task_id = 'get_order_prices',
        python_callable = get_order_prices
    )

    compute_sum = PythonOperator(
        task_id = 'compute_sum',
        python_callable = compute_sum
    )

    compute_average = PythonOperator(
        task_id = 'compute_average',
        python_callable = compute_average
    )

    display_result = PythonOperator(
        task_id = 'display_result',
        python_callable = display_result
    )


get_order_prices >> [compute_sum, compute_average] >> display_result



# Switch over to the Airflow UI

# Click through to the DAG in the GridView and show the tasks on the left

# Click on the GraphView and show the DAG (make sure you zoom out)

# Enable the DAG and enable Auto-refresh

# The DAG should run through fine

# Now back to the GridView

# IMPORTANT: Click on each task and show the Logs and the XCom - so we can see step by step how the data flows through



--------------------------------------------------------------------------------
# Version 04
--------------------------------------------------------------------------------


############ Notes ------------------------

# In this data pipeline, tasks are created based on Python functions using the @task decorator as shown below. The function name acts as a unique identifier for the task.

# The returned value will be made available for use in later tasks.

# All of the processing for adding XComs and pulling Xcoms is  being done in the new Airflow 2.0 DAG as well, but it is all abstracted from the DAG developer.

# All of the XCom usage for data passing between these tasks is abstracted away from the DAG author in Airflow 2.0. However, XCom variables are used behind the scenes and can be viewed using the Airflow UI as necessary for debugging or DAG monitoring.

# Similarly, task dependencies are automatically generated within TaskFlows based on the functional invocation of tasks. In Airflow 1.x, tasks had to be explicitly created and dependencies specified 

############ Notes ------------------------

# passing_data_with_taskflow.py


import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner' : 'loonycorn'
}



@dag(dag_id='cross_task_communication_taskflow',
     description = 'Xcom using the TaskFlow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once',
     tags = ['xcom', 'python', 'taskflow_api'])
def passing_data_with_taskflow_api():

    @task
    def get_order_prices():
        order_price_data = {
            'o1': 234.45, 
            'o2': 10.00, 
            'o3': 34.77, 
            'o4': 45.66, 
            'o5': 399
        }

        return order_price_data

    @task
    def compute_sum(order_price_data: dict):

        total = 0
        for order in order_price_data:
            total += order_price_data[order] 

        return total    

    @task
    def compute_average(order_price_data: dict):

        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order] 
            count += 1

        average = total / count

        return average


    @task
    def display_result(total, average):

        print("Total price of goods {total}".format(total=total))
        print("Average price of goods {average}".format(average=average))

    order_price_data = get_order_prices()

    total = compute_sum(order_price_data)
    average = compute_average(order_price_data)

    display_result(total, average)


passing_data_with_taskflow_api()




# Switch over to the Airflow UI

# Click through to the DAG in the GridView and show the tasks on the left

# Click on the GraphView and show the DAG (make sure you zoom out) - note that the DAG is based on how we have invoked the Python functions

# Enable the DAG and enable Auto-refresh

# The DAG should run through fine

# Now back to the GridView

# IMPORTANT: Click on each task and show the Logs and the XCom - so we can see step by step how the data flows through

# Note that the XComs are passed between tasks even though we haven't explicitly specified push and pull of XComs



--------------------------------------------------------------------------------
# Version 05
--------------------------------------------------------------------------------


############ Notes ------------------------

# When the return type of a task is a dictionary and the next input accepts a dict type that is explicitly specified then Airflow automatically understands that you are passing multiple outputs as a part of your XCom and things are interpreted correctly


############ Notes ------------------------

# Update the same file

# passing_data_with_taskflow.py


import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner' : 'loonycorn'
}



@dag(dag_id='cross_task_communication_taskflow',
     description = 'Xcom using the TaskFlow API',
     default_args = default_args,
     start_date = days_ago(1),
     schedule_interval = '@once',
     tags = ['xcom', 'python', 'taskflow_api'])
def passing_data_with_taskflow_api():

    @task
    def get_order_prices():
        order_price_data = {
            'o1': 234.45, 
            'o2': 10.00, 
            'o3': 34.77, 
            'o4': 45.66, 
            'o5': 399
        }

        return order_price_data


    @task
    def compute_total_and_average(order_price_data: dict):

        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order] 
            count += 1

        average = total / count

        return {'total_price': total, 'average_price': average}


    @task
    def display_result(price_summary_data: dict):

        total = price_summary_data['total_price']
        average = price_summary_data['average_price']

        print("Total price of goods {total}".format(total=total))
        print("Average price of goods {average}".format(average=average))

    order_price_data = get_order_prices()

    price_summary_data = compute_total_and_average(order_price_data)

    display_result(price_summary_data)


passing_data_with_taskflow_api()



# Switch over to the Airflow UI

# Click through to the DAG in the GridView and show the tasks on the left

# Trigger the DAG - things should run fine

# IMPORTANT: Click on each task and show the Logs and the XCom - so we can see step by step how the data flows through

# Note that multiple output dictionaries are passed between tasks



--------------------------------------------------------------------------------
# Version 06
--------------------------------------------------------------------------------

# Update the same file passing_data_with_taskflow.py


# Now let's say that display_result took in only the total and average


# Change only the display_result function to look like this:

    @task
    def display_result(total, average):

        print("Total price of goods {total}".format(total=total))
        print("Average price of goods {average}".format(average=average))


# Change the invocation of display_result to be like this:


    display_result(
        price_summary_data['total_price'], 
        price_summary_data['average_price'])


# Switch over to the Airflow UI

# Click through to the DAG in the GridView and show the tasks on the left

# Trigger the DAG - the last task for display result should show an error

# IMPORTANT: Click on each task and show the Logs and the XCom - so we can see step by step how the data flows through

# Note the error in the logs

# We cannot parse the individual fields of the dictionary outside of the task in this fashion


----------------------------------


# Now update compute_total_and_average decorator to be like this

    @task(multiple_outputs=True)
    def compute_total_and_average(order_price_data: dict):

        # No change to the body of this function





# Go to the GridView and run the DAG

# Everything should run just fine

# Click on the compute_total_and_average task and show the XCom

# Note that total_price and average_price are present as parameters in addition to the dictionary return value



