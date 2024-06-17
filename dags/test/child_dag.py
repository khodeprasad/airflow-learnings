from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'Prasad Khode',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    dag_id="child_dag",
    default_args=default_args,
    start_date=datetime(2024, 6, 16),
    catchup=False,
    schedule=None,
) as dag:
    
    child_task1 = DummyOperator(
        task_id='child_task1',
        dag=dag,
    )

    child_task2 = DummyOperator(
        task_id='child_task2',
        dag=dag,
    )

    child_task1 >> child_task2
