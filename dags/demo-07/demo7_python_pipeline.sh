> Create a folder "datasets" in root of airflow

airflow/datasets -> insurance.csv

# Show the insurance.csv file in Numbers so we can see the data (it has some null values spread out)

# in terminal
$ pip install pandas

# > Restart the shedular and webserver

# executing_python_pipeline_01.py

import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
   'owner': 'loonycorn'
}

def read_csv_file():
    df = pd.read_csv('/Users/loonycorn/airflow/datasets/insurance.csv')

    print(df)

    return df.to_json()


with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

read_csv_file


# > Explicitly trigger the code

# Go to the Airflow UI

# On the "Grid" view check the logs and the XCom


--------------------------------------------------------------------------


# executing_python_pipeline_02.py
import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
   'owner': 'loonycorn'
}

def read_csv_file():
    df = pd.read_csv('/Users/loonycorn/airflow/datasets/insurance.csv')

    print(df)

    return df.to_json()


def remove_null_values(**kwargs):
    ti = kwargs['ti']

    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    return df.to_json()


with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )
    
    read_csv_file >> remove_null_values


# > Explicitly trigger the code

# Go to the Airflow UI

# On the "Grid" view check the logs and the XCom

# Show the logs of both tasks - one will have null values, the other will not

# Observe the null values have been removed 


----------------------------------------------------------------------


# > Create a folder called "output" in airflow home directory

# airflow/output/


# executing_python_pipeline_03.py
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
   'owner': 'loonycorn'
}


def read_csv_file():
    df = pd.read_csv('/Users/loonycorn/airflow/datasets/insurance.csv')

    print(df)

    return df.to_json()


def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    return df.to_json()


def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(
        '/Users/loonycorn/airflow/output/grouped_by_smoker.csv', index=False)


def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean', 
        'charges': 'mean'
    }).reset_index()
    

    region_df.to_csv(
        '/Users/loonycorn/airflow/output/grouped_by_region.csv', index=False)


with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )
    
    groupby_smoker = PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
    )
    
    groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
    )

read_csv_file >> remove_null_values >> [groupby_smoker, groupby_region]


# Go to the "Graph" view and show the graph

# > Run the code 

# > Open each file in the output/ one by one. We can observe we have successfully grouped by age, region and if smoker or not















