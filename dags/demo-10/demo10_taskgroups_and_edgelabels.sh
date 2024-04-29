
# > Go to the variables tab (it should be open on a different tab)
# > Admin -> Variables -> Edit

# If not already set

Key: transform_action
Value: filter_by_northwest
Save


# Show the code

# branching_with_taskgroups_edgelabels.py

import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label


default_args = {
   'owner': 'loonycorn'
}

DATASETS_PATH = '/Users/loonycorn/airflow/datasets/insurance.csv'

OUTPUT_PATH = '/Users/loonycorn/airflow/output/{0}.csv'

def read_csv_file(ti):
    df = pd.read_csv(DATASETS_PATH)

    print(df)

    ti.xcom_push(key='my_csv', value=df.to_json())


def remove_null_values(ti):
    json_data = ti.xcom_pull(key='my_csv')
    
    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)

    ti.xcom_push(key='my_clean_csv', value=df.to_json())


def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)
    
    print(transform_action)

    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action == 'groupby_region_smoker':
        return "grouping.{0}".format(transform_action)


def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(key='my_clean_csv')
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

    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)


    
with DAG(
    dag_id = 'taskgroups_and_edgelabels',
    description = 'Running a branching pipeline with TaskGroups and EdgeLabels',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'taskgroups', 'edgelabels', 'branching']
) as dag:
    
    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:

        read_csv_file = PythonOperator(
            task_id='read_csv_file',
            python_callable=read_csv_file
        )

        remove_null_values = PythonOperator(
            task_id='remove_null_values',
            python_callable=remove_null_values
        )

        read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    with TaskGroup('filtering') as filtering:
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


    with TaskGroup('grouping') as grouping:
        groupby_region_smoker = PythonOperator(
            task_id='groupby_region_smoker',
            python_callable=groupby_region_smoker
        )
    
    reading_and_preprocessing >> Label('preprocessed data') >> determine_branch >> Label('branch on condition') >> [filtering, grouping]



# Note: The only changes to the code are we have explicitly specified the key for our
# xcom pushes and pulls. And at determine_branch be are specifying the task_ids by 
# mentioning the task group followed by task id.


# Open up the Airflow UI

# Go to the "Graph" view and show how the graph looks

# For each TaskGroup double-click and show the details (zoom out as needed)
# Double-click and close each TaskGroup when done

# > Run the code 

# Go to the "Grid" view and show what the results look like
# Show the output folder and the northwest.csv file
