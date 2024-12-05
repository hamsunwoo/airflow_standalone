from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

os.environ['LC_ALL'] = 'C'

def gen_emp(id, rule="all_success"):    
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
     'make_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['make', 'parquet'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
       
    task_check = BashOperator(
            task_id="check.done",
            bash_command="""
                DONE_FILE={{var.value.IMPORT_DONE_PATH}}/{{logical_date.strftime("%y%m%d")}}/_DONE
                bash {{var.value.CHECK_SH}} $DONE_FILE
            """
            )
    
    task_parquet = BashOperator(
            task_id="to.parquet",
            bash_command="""
                echo "to parquet"

                READ_PATH=~/data/csv/{{logical_date.strftime("%y%m%d")}}/csv.csv
                SAVE_PATH=~/data/parquet

                #mkdir -p $SAVE_PATH

                python ~/airflow/dags/to_parquet.py $READ_PATH $SAVE_PATH
            """
            )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err"
            """,
            trigger_rule="one_failed"
            )

    task_make = BashOperator(
            task_id="make.done",
            bash_command="""
                echo "make done"
                MAKE_DONE_PATH=~/data/done/make_done
                MAKE_DONE_FILE=$MAKE_DONE_PATH/_DONE

                mkdir -p ~/data/done/make_done
                touch $MAKE_DONE_PATH/_DONE
                
                bash ~/airflow/dags/make_done.sh ~/data/parquet

            """
             )

    
    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')

    task_start >> task_check
    task_check >> task_parquet >> task_make
    task_check >> task_err >> task_end

    task_make >> task_end


