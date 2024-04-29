# Notes:

# In Airflow, "catch up" refers to the process of scheduling and executing all the past DAG runs that would have been scheduled if the DAG had been created and running at an earlier point in time. When catch up is enabled, Airflow will create DAG runs for all past intervals that have not yet been executed, and will execute them in sequence until it reaches the current interval.

# "Backfill," on the other hand, refers specifically to the process of executing past DAG runs for a specific date range, rather than all past DAG runs. Backfill can be useful if you need to rerun a subset of past DAG runs, for example, if you made changes to your DAG definition or if some of your tasks failed to complete properly.

# Each field in a cron expression represents a different aspect of the date and time for the job to run. Here's what each field represents:

# Minute (0-59): Specifies the minute of the hour when the job should run. For example, "0" means the top of the hour, "30" means 30 minutes past the hour, and so on.

# Hour (0-23): Specifies the hour of the day when the job should run. For example, "0" means midnight, "12" means noon, and so on.

# Day of the month (1-31): Specifies the day of the month when the job should run. For example, "1" means the first day of the month, "15" means the 15th day of the month, and so on.

# Month (1-12 or Jan-Dec): Specifies the month when the job should run. For example, "1" or "Jan" means January, "2" or "Feb" means February, and so on.

# Day of the week (0-6 or Sun-Sat): Specifies the day of the week when the job should run. For example, "0" or "Sun" means Sunday, "1" or "Mon" means Monday, and so on.

# (Optional) Year (e.g. 2023): Specifies the year when the job should run. This field is optional, and is typically not used unless the job needs to run on a specific year.

# Each field can be set to a single value, a range of values, or a list of values. You can also use special characters to specify certain patterns, such as "*" for "every" or "/" for "every X". For example, "0 0 * * * *" means to run the job every minute of every day.

# Minute
# Hour
# Day of the month
# Month
# Day of the week
# Year (optional)

# This cron expression means "run the task at 00:00 (midnight) every day". The first "0" represents the minute, the second "0" represents the hour, and the " * * *" parts represent "every day, every month, and every day of the week", respectively.

-----------------------------------------------------------------------------------------

# executing_cron_catchup_backfill_01.py
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
   'owner' : 'loonycorn'
}

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='taskChoose'):
        return 'taskC'
    else:
        return 'taskD'   

def task_c():
    print("TASK C executed!")


with DAG(
    dag_id = 'cron_catchup_backfill',
    description = 'Using crons, catchup, and backfill',
    default_args = default_args,
    start_date = days_ago(5),
    schedule_interval = '0 0 * * *',
    catchup = True
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD



# Above we have created a simple DAG lets run this, also observe we have set "start_date = days_ago(5)"
# By default catchup will be True (also explicitly set to true)

# Goto browser and refresh

# Click on the "cron_catchup_backfill"

# Show the "Graph" view so we can see the conditions

# Assuming we are in the "Grid View" we can see there is 3 tasks ready for execution

# Toggle the ON switch click "Auto-refresh" if not selected

# Once the execution is complete

# We can observe that from the past 5 days DAGs have been run

# Next click on the "Calander View" we can see all the days when the DAG was run
# this is happening because we have set catchup to True


-------------------------------------------------------------

# Delete the DAG from the Airflow UI (so we can start from scratch)

# This cron expression means "run the task every 12 hours on Saturdays at midnight". The "0" in the first field means "at the start of the hour", the "*/12" in the second field means "every 12 hours", the "6" in the fifth field means "on Saturdays", and the "0" in the sixth field means "at midnight". The third and fourth fields, " * *", represent "every day and every month", respectively.

# So, if you set a DAG to use this cron expression as the schedule_interval, the DAG will run the task every 12 hours on Saturdays at midnight. For example, the task will run at midnight and at noon on Saturdays.

# Just change the cron expression

'0 */12 * * 6 0'

# and the days_ago()

days_ago(30)


# Above we have created a simple DAG lets run this, also observe we have set "start_date = days_ago(5)"
# By default catchup will be True (also explicitly set to true)

# Goto browser and refresh

# Click on the "cron_catchup_backfill"

# Assuming we are in the "Grid View" we can see there is 3 tasks ready for execution

# Toggle the ON switch click "Auto-refresh" if not selected

# Watch as the DAG catches up

# Show the Calendar - this runs only on the weekends

-------------------------------------------------------------

# Delete the DAG from earlier (for a fresh start)

# Make only one change

catchup = False


# Goto browser and refresh
# Click on the "cron_catchup_backfill"

# Assuming we are in the "Grid View" we can see there is 3 tasks ready for execution

# Toggle the ON switch click "Auto-refresh" if not selected

# We can observe here only one run has been complete ie of today

# Click on the Calendar view and show this

-------------------------------------------------------------

# Open a new terminal
$ conda activate /opt/anaconda3/envs/python37

$ airflow dags -h
# This will list all the CLI command avaliable for DAG

$ airflow dags backfill -h
# This will show all the argument that has to be put up to run backfill command
# airflow dags backfill -s START_DATE -e END_DATE DAG_ID

$ cal
# This will display the calander in terminal
# Lets say we want to backfill from 14 - 17 DEC 2022
# Today is 20/12/2022

$ airflow dags backfill -s 2022-04-01 -e 2022-04-20 cron_catchup_backfill
# Once the execution is complete with the following output
# {backfill_job.py:899} INFO - Backfill done for DAG <DAG: understanding_catchup_backfill_v3>. Exiting.


# Goto browser and refresh

# Assuming we are in the "Grid View"

# Make sure that Auto-refresh is on, hit refresh if you cannot see anything

# Observe now we have 4 extra runs with a reverse arrow on them if we hover on the runs
# we can observe the type is backfill

# Now click on "Calander View" and see the details





