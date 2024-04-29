import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
   'owner': 'PK'
}

def read_csv_file():
    df = pd.read_csv('/Users/gangaprasadkhode/airflow/dags/demo-07/versions/datasets//insurance.csv')

    print(df)

    return df.to_json()


with DAG(
    dag_id = 'd07_python_pipeline_01',
    description='Running a Python pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

read_csv_file