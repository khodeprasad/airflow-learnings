# IMPORTANT: Behind the scenes use Cmd + L to clear the sqllite screen


# Let's create a SQLlite database that we can use

> cd ~/airflow/

> mkdir database

> touch my_sqlite.db

> sqlite3 my_sqlite.db

> .databases

> .tables

----------------------------------------------------------------------

# Now lets get back to creating the code for SQLOperator

# > Goto "localhost:8080"
# > Click on the "Admin" tab in the top navigation bar.
# > Click on "Connections" in the dropdown menu.
# > Click on the "Create" button on the right side of the page.

Conn Id: my_sqlite_conn
Conn Type: sqlite
Host: /Users/loonycorn/airflow/database/my_sqlite.db

# Note: "my_sqlite.db" is a sqlite DB which we just created

# > Click "Test" 
# > Click "Save"


----------------------------------------------------------------------

# https://airflow.apache.org/docs/apache-airflow-providers-sqlite/stable/_api/airflow/providers/sqlite/operators/sqlite/index.html


# executing_sql_pipeline_01.py

from airflow import DAG

from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'PK'
}

with DAG(
    dag_id = 'executing_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

create_table


# > Run the DAG once success

# Show the Logs and the create table has run through

# > Go back to the terminal and run these commands

> .tables

> .schema users

> SELECT * FROM users;

----------------------------------------------------------------------


# executing_sql_pipeline_02.py

from airflow import DAG

from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'PK'
}

with DAG(
    dag_id = 'executing_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )


    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"""SELECT * FROM users""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True
    )


create_table >> [insert_values_1, insert_values_2] >> display_result


# Back to the UI

# Show the "Graph" view

# Trigger the DAG

# Click on the "display_result" task and show the XCom

# Switch to the terminal window

> SELECT * FROM users;

# 1|Julie|30|0||2023-04-23 04:39:07
# 2|Peter|55|1||2023-04-23 04:39:07
# 3|Emily|37|0||2023-04-23 04:39:07
# 4|Katrina|54|0||2023-04-23 04:39:07
# 5|Joseph|27|1||2023-04-23 04:39:07
# 6|Harry|49|1||2023-04-23 04:39:16
# 7|Nancy|52|1||2023-04-23 04:39:16
# 8|Elvis|26|1||2023-04-23 04:39:16
# 9|Mia|20|1||2023-04-23 04:39:16


# We will recreate this table

> DROP TABLE users;


-------------------------------------------------------------------------
from airflow import DAG

from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'PK'
}

with DAG(
    dag_id = 'executing_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    city VARCHAR(50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )


    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r"""
            DELETE FROM users WHERE is_active = 0;
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r"""
            UPDATE users SET city = 'Seattle';
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"""SELECT * FROM users""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True
    )


create_table >> [insert_values_1, insert_values_2] >> delete_values >> update_values >> display_result


# Back to the UI

# Show the "Graph" view

# Trigger the DAG


> SELECT * FROM users;

# 2|Peter|55|Seattle|1|2023-04-23 04:56:32
# 5|Joseph|27|Seattle|1|2023-04-23 04:56:32
# 6|Harry|49|Seattle|1|2023-04-23 04:56:45
# 7|Nancy|52|Seattle|1|2023-04-23 04:56:45
# 8|Elvis|26|Seattle|1|2023-04-23 04:56:45
# 9|Mia|20|Seattle|1|2023-04-23 04:56:45


----------------------------------------------------------------------


# Go to the Airflow UI

> Admin -> Variables -> Add

Key: emp
Value: employees
Save

Key: dept
Value: departments
Save

Key: emp_dept
Value: employees_departments
Save


--------------------------------------------------------------------------------------


# > create a folder called "sql_statements" in airflow home

# airflow/sql_statements

# Show the contents of the SQL file in this order

# create_employee_table.sql
# create_department_table.sql
# insert_data_employees.sql
# insert_data_departments.sql
# join_table.sql
# display_emp_dept.sql




# DAG_using_SQLOperator_v5.py
from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago
from time import sleep
from airflow.models import Variable

default_args = {
    'owner': 'loonycorn'
}

employees_table = Variable.get("emp", default_var=None)
departments_table = Variable.get("dept", default_var=None)
employees_departments_table = Variable.get("emp_dept", default_var=None)



with DAG(
        'SqliteOperator_v5',
        description = 'Here we are creating a simple pipeline using SqliteOperator.',
        default_args = default_args,
        start_date = days_ago(1),
        schedule_interval = '@daily',
        template_searchpath = '/Users/loonycorn/airflow/sql_statements'
) as dag:
    create_employees_table = SqliteOperator(
        task_id='create_employees_table',
        sql='create_employee_table.sql',
        params={'employees_table': employees_table},
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )

    create_departments_table = SqliteOperator(
        task_id='create_departments_table',
        sql='create_department_table.sql',
        params={'departments_table': departments_table},
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )

    insert_data_employees = SqliteOperator(
        task_id='insert_data_employees',
        sql='insert_data_employees.sql',
        params={'employees_table': employees_table},
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )

    insert_data_departments = SqliteOperator(
        task_id='insert_data_departments',
        sql='insert_data_departments.sql',
        params={'departments_table': departments_table},
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )

    join_tables = SqliteOperator(
        task_id='join_tables',
        sql='join_table.sql',
        params={'employees_departments_table': employees_departments_table},
        sqlite_conn_id='my_sqlite_conn',
        dag=dag
    )

    result_sql = SqliteOperator(
        task_id='result_sql',
        sql='display_emp_dept.sql',
        params={'employees_departments_table': employees_departments_table},
        sqlite_conn_id='my_sqlite_conn',
        dag=dag,
        do_xcom_push = True
    )

    create_employees_table >> insert_data_employees
    create_departments_table >> insert_data_departments
    [insert_data_employees, insert_data_departments] >> join_tables >> result_sql



# Go to the "Graph" view in the UI 

# > run the code once 

# > Click on "result_sql" > "Xcoms"

# We can see the result from the query


# > Goto the terminal


> .tables
# departments            employees_departments
# employees              users 



> SELECT * FROM employees;

> SELECT * FROM departments;

> SELECT * FROM employees_departments;
# John Doe|Analyst|Sales
# Peter Smith|Manager|Operations
# Emily Johnson|Software Engineer|Sales
# Katrina Wilson|Scientist|Operations




