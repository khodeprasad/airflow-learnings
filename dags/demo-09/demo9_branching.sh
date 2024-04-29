# Simple branching


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
    'owner' : 'loonycorn',
}

def has_driving_license():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'eligible_to_drive'
    else:
        return 'not_eligible_to_drive'                      

def eligible_to_drive():
    print("You can drive, you have a license!")

def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need a license to drive")


with DAG(
    dag_id = 'executing_branching',
    description = 'Running branching pipelines',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'conditions']
) as dag:

    taskA = PythonOperator(
        task_id = 'has_driving_license',
        python_callable = has_driving_license              
    )

    taskB = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'eligible_to_drive',
        python_callable = eligible_to_drive              
    )

    taskD = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive              
    )

taskA >> taskB >> [taskC, taskD]



# Goto browser and refresh

# Click on the "executing_branching"

# Show the "Graph" view so we can see the conditions

# Trigger the DAG in the Graph view and show that one of the branches is chosen

# Switch to the "Grid" view and re-run the graph 3-4 times

# Show that a different task is executed each time


--------------------------------------------------------------------------

# Make sure that datasets/insurance.csv is present in the airflow/datasets/ folder

# > Delete all the files inside the "output" directory

> Go back to "localhost:8080"

# IMPORTANT: Open up Admin -> Variables in a new tab

# > Admin -> Variables -> Add

Key: transform_action
Value: filter_by_southwest
Save


# executing_branching.py

import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}

DATASETS_PATH = '/Users/loonycorn/airflow/datasets/insurance.csv'

OUTPUT_PATH = '/Users/loonycorn/airflow/output/{0}.csv'

def read_csv_file():
    df = pd.read_csv(DATASETS_PATH)

    print(df)

    return df.to_json()


def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    return df.to_json()

def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)
    
    if transform_action.startswith('filter'):
        return transform_action
    elif transform_action == 'groupby_region_smoker':
        return 'groupby_region_smoker'


def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)


    
with DAG(
    dag_id = 'executing_branching',
    description = 'Running a branching pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline', 'branching']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_southwest = PythonOperator(
        task_id='filter_by_southwest',
        python_callable=filter_by_southwest
    )
    
    filter_by_southeast = PythonOperator(
        task_id='filter_by_southeast',
        python_callable=filter_by_southeast
    )

    filter_by_northwest = PythonOperator(
        task_id='filter_by_northwest',
        python_callable=filter_by_northwest
    )

    filter_by_northeast = PythonOperator(
        task_id='filter_by_northeast',
        python_callable=filter_by_northeast
    )

    groupby_region_smoker = PythonOperator(
        task_id='groupby_region_smoker',
        python_callable=groupby_region_smoker
    )
    
    read_csv_file >> remove_null_values >> determine_branch >> [filter_by_southwest, 
                                                                filter_by_southeast, 
                                                                filter_by_northwest,
                                                                filter_by_northeast,
                                                                groupby_region_smoker]


# Go to the Airflow UI and show the "Graph" page - zoom out so all the branches can be seen

# > run the code once and show that only branch is executed and is in green

# Observe "southwest.csv" file is generated in output folder

# Open the file and show that all records are for the southwest


----------------------------------------------------------------------------------------

# > Go back to "localhost:8080"
# > Admin -> Variables -> Edit


Key: transform_action
Value: groupby_region_smoker
Save


# > run the code once 

# Observe the files in output/
# grouped_by_smoker.csv
# grouped_by_region.csv

# Open them and show in Numbers





















