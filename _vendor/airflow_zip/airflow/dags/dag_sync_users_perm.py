#modified1
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.version import version
from datetime import datetime, timedelta

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('SyncUsersPerm',
         start_date=datetime(2021,8,11),
         max_active_runs=3,
         schedule_interval='*/5 * * * *',  # sync variable every 5 minutes
         default_args=default_args,
         catchup=False,
         is_paused_upon_creation=False
         ) as dag:

    t_start = DummyOperator(
        task_id='start'
    )
    
    t_end = DummyOperator(
        task_id='end'
    )
    
    with TaskGroup('SyncUsersPerm') as pipeline_task_group:
        # Import variables the are uploaded to the dag directory
        task1 = BashOperator(
            task_id="sync_users_and_roles",
            bash_command='scripts/associate_users_and_roles.sh' 
        ) 
    t_start >> pipeline_task_group >> t_end
    
