------------------------------------------------------------------------------

# First lets remove all the inbuild examples

# Terminate the scheduler and webserver if running using "control+c"

# Goto "airflow.cfg"

# Scroll down to "load_examples"

load_examples = False

# Save the file and close

# In terminal with 2 tabs

# Kill the original scheduler and re-run
$ airflow scheduler

# Kill the original webserver and re-run
$ airflow webserver

# In the browser goto "localhost:8080"

# Login again

# Observe now we dont have any of the inbuild examples

------------------------------------------------------------------------------

# On the terminal window run
$ conda activate /opt/anaconda3/envs/python37

# This is the location that airflow expects all our DAGs to be in
$ airflow config get-value core dags_folder


# Now open the "airflow" folder is "vscode" present in "~/airflow"

# So we have to create a folder called "dags" in "~/airflow/"

# Once the "dags" folder is created we will be creating al of our DAGs inside this folder



# https://towardsdatascience.com/apache-airflow-547339588c29
# https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html
# https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html#airflow.models.baseoperator.BaseOperator

# Create a new file called "simple_hello_world.py" inside "dags" folder

# simple_hello_world_01.py


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'loonycorn',
}

dag = DAG(
    dag_id='hello_world',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = None,
    schedule_interval = None,
)

task = BashOperator(
    task_id = 'hello_world_task',
    bash_command = 'echo Hello world!',
    dag = dag
)

task


--------------------------------------------------------------------------

# Now go back to the browser at "localhost:8080"

# Now observe we have a new DAG called "hello_world" with "loonycorn" as its owner 
# scheduled "None" and the next run set to no time

# Click on the "hello_world"

# Now goto the "Graph View" here we see one operator called "hello_world_task" which 
# is a BashOperator. Next we can click on "Details" to see all the details.cfg

# Click on Code to show that the code that you wrote is here


# Now get go back to "Grid View" and toggle the ON switch click "Auto-refresh" if not selected


# Now since this task is not scheduled we will have to explicitly trigger it

# Click on the Play -> Trigger DAG button on the top-right to run this task

# It should run through successfully

# Now we can see all the DAG details of the run in the "Grid View" just refresh the page once


# now observe on the top right cornor the next run is not scheduled

# Now lets switch to "Graph View" here click on the "hello_world_task" -> click on "Log"

# Now scroll down a bit we can observe we have succesfully printed HELLO WORLD!

# Next click on "Rendered Templates" to see the command executed

# Back to the Grid View and run this through again


--------------------------------------------------------------------------


# Let's change the code in dags/ folder just a little bit

# Change the schedule interval and add tags for the DAG

    schedule_interval = '@daily',
    tags = ['beginner', 'bash', 'hello world']


# Change what the bash operator prints out to screen

    bash_command = 'echo Hello world once again!',

# Go to the main Airflow UI which lists the DAGs

# Show that tags have been added to this DAG

# Also show that it has executed twice successfully (Hover over the 2 in the circle)


# On the UI go to "Code" and show that the new code has been automatically picked up

# Show that the Schedule and Next Run on the top right has changed

# On the "Grid" view trigger the DAG once again

# Select the latest run with the new message and parameters using the graphs on the left pane

# Click on the small green square of the task

# Click on the "Logs" tab

# Show the updated message Hello world once again!

---------------------------------------------------------------------------

# There is another way of running the same above DAG as shown below

# Change the code to be as shown below

# simple_hello_world.py (simple_hello_world_02.py)

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'loonycorn'
}

with DAG(
    dag_id='hello_world',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

    task = BashOperator(
        task_id = 'hello_world_task',
        bash_command = 'echo Hello - created a DAG using with!'
    )

task


# On the Airflow UI 
# Click on the "hello_world"
# Go to Code and show the new code has been picked up

# On the Grid view run the DAG once again

# Select the task and show the logs

# See that Hello - created a DAG using with! is printed out

# Go to the terminal to the airflow/ folder

$ cd logs

$ ls -l 

# Show that the logs for the tasks are present here






















