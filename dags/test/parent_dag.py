from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


default_args = {
    'owner': 'Prasad Khode',
    'depends_on_past': False,
    'start_date': days_ago(1),
}


with DAG(
    dag_id="parent_dag",
    default_args=default_args,
    start_date=datetime(2024, 6, 16),
    catchup=False,
    schedule="*/5 * * * *",
) as dag:

    parent_task1 = DummyOperator(
        task_id='parent_task1',
        dag=dag,
    )
    parent_task2 = DummyOperator(
        task_id='parent_task2',
        dag=dag,
    )

    child_trigger = TriggerDagRunOperator(
        task_id="child_trigger_dag_run",
        trigger_dag_id="child_dag",
    )

    parent_task1 >> parent_task2 >> child_trigger
