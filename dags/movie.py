from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

os.environ['LC_ALL'] = 'C'

with DAG(
     'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:

       
    task_get_data = BashOperator(
            task_id="get.data",
            bash_command="""
                echo "get data"
            """
            )
    
    task_save_data = BashOperator(
            task_id="save.data",
            bash_command="""
                echo "save data"
            """    
            )

        
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end',trigger_rule="all_done")

    task_start >> task_get_data >> task_save_data
    task_save_data >> task_end
