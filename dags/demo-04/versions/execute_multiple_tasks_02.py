from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
   'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'd04_executing_multiple_tasks_02',
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

