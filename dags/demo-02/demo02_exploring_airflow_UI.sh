# https://airflow.apache.org/docs/apache-airflow/stable/ui.html
# https://docs.astronomer.io/learn/airflow-ui

# Have the UI open

# Login with these credentials

cloud.user
password

# Observe here we are in the DAG view this is where all the DAGs will be listed
# Now observe there are multiple airflow examples that has been created for us by airflow


# Scroll and show the different examples listes
# Hover over the circles under Run
# Hover over a few circles under "Recent Tasks"
# Hover over the 3 dots at the end and show the options


------------------------------------------------------------------
# Let's look at one of these examples
# Click and goto "example_branch_operator"

# Toggle the ON button of ON button for "example_branch_operator" DAG on the top left

# Assuming we are in the "Grid View"

# Once the run is complete we will get a summary of the run and we can see in detail which tasks where run and which was skipped using the color code provided

# Now lets click on the "play button" on the top right and click "Trigger DAG" to run the DAG again

# (Note: if the Auto-refresh is off turn it ON)

# Observe now to the left pane we have a bar graph like projection saying how much time it took to execute 
# each specific runs and all the tasks which where run and the others whick where skipped
# (Also note on the top we can filter out based on the date when run and to display how many runs 
# in the current page and run types and run states)

# Note: Each column represents a DAG run and each square represents a task instance in that DAG run


# Click on the first bar from the bar chart we can get details of that run on the right side

# Now click on the second bar for the second run

# Click on the "branching" task of the second run and show the task "Details"

# Click on the "Logs" tab and show the logs generated

------------------------------------------------------------------


# Now click on the "Graph View" from the top pane here we can get a more comprehensive view of the DAG 

# Zoom out a bit using your mousepad and show the entire DAG

# we can see different status of each task using the color code and also understand what operators are 
# used for each task in this DAG the green ones are EmptyOperators and the pink ones are BranchPythonOperators.
# This view is particularly useful when reviewing and developing a DAG.

# Hover over some of the DAG paths and show the tooltip popup


# Now click on the "Calendar View" here we can get a overview of the DAGs history for days, months, years to look at all the successes and failures

# Hover over the two successful runs and show tooltip
