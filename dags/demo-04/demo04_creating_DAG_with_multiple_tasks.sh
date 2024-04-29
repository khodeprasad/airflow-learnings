# Create a new file called "execute_multiple_tasks.py" inside "dags" folder

# @once: Run the DAG only once. This is useful when you want to trigger the DAG manually or when you want to schedule the DAG using an external trigger.

# execute_multiple_tasks_01.py


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'executing_multiple_tasks',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'echo TASK B has executed!'
    )

taskA.set_downstream(taskB)


# Goto browser and refresh
# Click on the "executing_multiple_tasks"

# Goto "Graph View" observe here we have taskA with is the upstream followed by taskB which
# is the downstream been set ie first taskA is executed then taskB is executed

# Toggle the ON switch click "Auto-refresh" if not selected

# Explicitly run the task if it does not run

# Now click on the "taskA" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed TASK A is executed!

# Go back and click on the "taskB" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed TASK B is executed!


-----------------------------------------------------------------------


# Now make one small change to the code

taskA.set_upstream(taskB)


# Back to the airflow UI and click on "Graph"

# Show that task B now has to run before task A

# In the "Grid" view explicitly trigger a run

# Notice that the task order on the left side has been swapped

# Hover over taskB and then taskA and show that the start time of B is before A


-----------------------------------------------------------------------

# Update the same code (we won't keep changing the DAG, let's use the same DAG)

# executing_multiple_tasks_02.py


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'executing_multiple_tasks',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags = ['upstream', 'downstream']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = '''
            echo TASK A has started!
            
            for i in {1..10}
            do
                echo TASK A printing $i
            done

            echo TASK A has ended!
        '''
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = '''
            echo TASK B has started!
            sleep 4
            echo TASK B has ended!
        '''
    )

    taskC = BashOperator(
        task_id = 'taskC',
        bash_command = '''
            echo TASK C has started!
            sleep 15
            echo TASK C has ended!
        '''
    )
    
    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D completed!'
    )

taskA.set_downstream(taskB)
taskA.set_downstream(taskC)

taskD.set_upstream(taskB)
taskD.set_upstream(taskC)


# Go to the main Airflow page and refresh

# Observe that our DAG now has tags

# Click through and notice that the Grid view has additional tasks

# Goto the "Graph View" and observe its flow

# Stay on the "Graph" view and trigger a run - show how the colors change

# Go to the "Grid" view (easier to see logs here)

# Now click on the "taskA" -> click on "Log"
# Now scroll down a bit we can observe we have succesfully printed 
# {subprocess.py:93} INFO - TASK A has started!
# {subprocess.py:93} INFO - TASK A printing 1
# {subprocess.py:93} INFO - TASK A printing 2
# {subprocess.py:93} INFO - TASK A printing 3
# {subprocess.py:93} INFO - TASK A printing 4
# {subprocess.py:93} INFO - TASK A printing 5
# {subprocess.py:93} INFO - TASK A printing 6
# {subprocess.py:93} INFO - TASK A printing 7
# {subprocess.py:93} INFO - TASK A printing 8
# {subprocess.py:93} INFO - TASK A printing 9
# {subprocess.py:93} INFO - TASK A printing 10
# {subprocess.py:93} INFO - TASK A has ended!

# Go back and click on the "taskB" -> click on "Log"

# Now scroll down a bit we can observe we have succesfully printed
# {subprocess.py:93} INFO - TASK B has started!
# {subprocess.py:93} INFO - TASK B has ended!

# Also observe the task b started and ended time there is a gap of 4 seconds

# Do the same with taskC and taskD of observing the logs

---------------------------------------------------------------------------------

# https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#task-dependencies
# There is another way of setting task dependencies


# Just replace the last 5 lines of code and refresh the browser and re-run the DAG
# execute_multiple_tasks_03.py


taskA >> taskB
taskA >> taskC

taskD << taskB
taskD << taskC

# Go to the main Airflow page

# Click through to the executing_multiple_tasks DAG

# Go to the "Code" and show that we now have the new code

# Go to "Graph" and show that the it is the same graph as before

# Run the graph from right here manually

-----------------------------------------------------------------------------------


# Just replace the last 5 lines of code and refresh the browser and re-run the DAG


taskA >> [taskB, taskC]

taskD << [taskB, taskC]



# Click through to the executing_multiple_tasks DAG

# Go to the "Code" and show that we now have the new code

# Go to "Graph" and show that the it is the same graph as before

# Run the graph from right here manually


-----------------------------------------------------------------------------------
# In the next example lets see how to generate skip, failure and success using bash commands

# https://linuxhint.com/bash_exit_on_error/#:~:text=An%20exit%20status%20code%20is,task%20by%20using%20shell%20script.


# Create a new folder called bash_scripts/ under dags/


# Place the scripts for taskA through taskG in that folder

# Show the contents of the scripts one by one (you do not need to paste in the contents)


# Now show the updated contents of executing_multiple_tasks_04.py

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'executing_multiple_tasks',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags = ['scripts', 'template search'],
    template_searchpath = '/Users/loonycorn/airflow/dags/bash_scripts'
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'taskA.sh'
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'taskB.sh'
    )

    taskC = BashOperator(
        task_id = 'taskC',
        bash_command = 'taskC.sh'
    )
    
    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'taskD.sh'
    )

    taskE = BashOperator(
        task_id = 'taskE',
        bash_command = 'taskE.sh'
    )

    taskF = BashOperator(
        task_id = 'taskF',
        bash_command = 'taskF.sh'
    )

    taskG = BashOperator(
        task_id = 'taskG',
        bash_command = 'taskG.sh'
    )

taskA >> taskB >> taskE

taskA >> taskC >> taskF

taskA >> taskD >> taskG


# Observe we have provided "template_searchpath = '/Users/loonycorn/airflow/dags/bash_scripts'" in DAG
# and we are calling the file "taskA.sh" in taskA we are skipping taskB using exit code 99
# and we are failing taskC with exis code 130


# Go to the browser and refresh the page 

# Go to the "Graph" view and observe the graph

# Run the DAG from here

# Task A, D, and G should be green (success) - hover over each and show

# Task C should be red, F should be orange - hover over each and show

# Task B and E should be pink - hover over each and show






















