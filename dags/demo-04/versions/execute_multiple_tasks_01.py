from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'd04_executing_multiple_tasks_01',
    description = 'DAG with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['upstream', 'downstream']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'echo TASK B has executed!'
    )

taskA.set_upstream(taskB)
