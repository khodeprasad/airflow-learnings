## ---- Notes
# Use these links for comments on installation
# https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi
# https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
## --------

# Open a terminal
$ python --version
# We are running Python 3.9.13

# https://airflow.apache.org/docs/apache-airflow/2.0.1/installation.html
# It is recomended to use python version 3.6, 3.7 or 3.8

# Airflow is tested with 3.7, 3.8, 3.9, 3.10

# So since we are using Anaconda, Open "Anaconda Navigator"
# From the left pane select "Enviroments"

# Observe here we have one enviroment ie the "base(root)" ie python version 3.9.13

# Click "+ Create" button on the bottom of the screen from the left pane

name -> python37
packages -> python
version -> some latest version

# Click "Create". It will take a few seconds

# Open a terminal
$ conda env list
# This will list all the enviroment present

# Now lets switch the enviroment from 3.9 to 3.7
$ conda activate /opt/anaconda3/envs/python37

$ python --version
# We are running Python 3.7.15

# https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html

# In terminal
$ pip install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.7.txt"
# It will take a few minutes to install


-------------------------------------------------------------------------------

$ airflow version

$ airflow -h
# This will list all the avaliable commands using airflow

$ airflow cheat-sheet

$ airflow info
# Show information about current Airflow and environment
# Observe airflow_home is in /Users/loonycorn/airflow in our case

$ cd /Users/loonycorn/airflow

$ ls -l

# Observe we have airflow.cfg this is the main configuration file

# IMPORTANT: Open airflow.cfg file in sublime text for better understanding


#### ------- Notes
# https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html

# Note the sections in the config files specified in []
#### ------- 

# For now we can observe "dags_folder = /Users/loonycorn/airflow/dags" this is where our DAG's will exist
# If we want to change the location we can do it here 

# https://medium.com/international-school-of-ai-data-science/executors-in-apache-airflow-148fadee4992
# https://docs.astronomer.io/learn/airflow-executors-explained
# Also note "executor = SequentialExecutor" 

# If we scroll down to the database section this is where we can change or include the database configurations

# Below database if the logging section this is where all our log file will be saved "base_log_folder = /Users/loonycorn/airflow/logs"

# Now lets scroll down to webserver here we can change the webserver configuration for now 
# it is "base_url = http://localhost:8080" we can change the port if we need and we can change any 
# other configuration of webserver if we need


# You can run specific commands to access the settings in airflow

$ airflow config --help

$ airflow config get-value core default_task_retries

$ airflow config get-value webserver base_url

$ airflow config get-value database sql_alchemy_conn


# Airflow requires a database. If you're just experimenting and learning Airflow, you can stick with the default SQLite option. If you don't want to use SQLite, then take a look at Set up a Database Backend to setup a different database.

# Lets create a database
$ airflow db init

# This may give you a warning about SQLAlchemy
# https://github.com/apache/airflow/issues/28723
# Airflow is currently not compatible with SQLAlchemy 2.0 which is about to be released. We need to make a deliberate effort to support it.



$ ls -l
# Observe here there is a sqlite DB create called airflow.db


$ airflow users -h


$ airflow users list
# Observe there is no users present so we have to create a user


$ airflow users create -h
# Observe here we have got a template as to how to create a user

$ airflow users create \
-e cloud.user@loonycorn.com \
-f CloudUser \
-l Loonycorn \
-p password \
-r Admin \
-u loonycorn 

$ airflow users list
# id | username  | email                    | first_name | last_name | roles
# ===+===========+==========================+============+===========+======
# 1  | loonycorn | cloud.user@loonycorn.com | loonycorn  | loonycorn | Admin

$ airflow scheduler

# If the below question is asked type "y" and hit the return key
# Please confirm database initialize (or wait 4 seconds to skip it). Are you sure? [y/N]

# Scheduler (as we saw above in cfg file we are using SequentialExecutor) monitors all tasks 
# and DAGs and stays in sync with a folder for all DAG objects to
# collects DAG parsing results to trigger active tasks

# Open another tab in the terminal
$ conda activate /opt/anaconda3/envs/python37

$ airflow webserver

# If the below question is asked type "y" and hit the return key
# Please confirm database initialize (or wait 4 seconds to skip it). Are you sure? [y/N]

# Observe the host "Host: 0.0.0.0:8080"








