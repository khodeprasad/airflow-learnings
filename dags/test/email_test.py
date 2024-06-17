from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'Prasad Khode',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='send_email_dag',
    default_args=default_args,
    description='A DAG to send email using the EmailOperator',
    schedule_interval='*/5 * * * *',
    catchup=False,
)

email_task = EmailOperator(
    task_id='send_email',
    to='test@test.com',
    subject='Airflow Email Test',
    html_content='<p>This is a test email sent from Airflow DAG.</p>',
    dag=dag,
)

email_task
