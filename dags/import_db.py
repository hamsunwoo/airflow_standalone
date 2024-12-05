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

with DAG(
     'import_db',
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
    tags=['import_db'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
       
    task_check = BashOperator(
            task_id="check",
            bash_command="""
                echo "check"
                CHECK_FILE=~/data/done
                bash {{ var.value.CHECK_SH }} $CHECK_FILE/{{logical_date.strftime("%y%m%d")}}/_DONE
            """,
            trigger_rule="all_success"
            )
    
    task_csv = BashOperator(
            task_id="to.csv",
            bash_command="""
                echo "to csv"
                U_PATH=~/data/count/{{logical_date.strftime("%y%m%d")}}/count.log
                CSV_PATH=~/data/csv/{{logical_date.strftime("%y%m%d")}}

                mkdir -p ${CSV_PATH}
                #cat $U_PATH | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv
                cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_PATH}/csv.csv
                #cat $U_PATH | awk '{print "\\"{{ds}}\\",\\"" $2 "\\",\\"" $1 "\\""}' > ${CSV_PATH}/csv.csv
            """
            )
    task_create_tbl = BashOperator(
                 task_id="create.table",
                 bash_command="""
                    SQL={{var.value.SQL_PATH}}/create_db_table.sql
                    MYSQL_PWD='{{var.value.DB_PASSWD}}' mysql -u root < $SQL
                 """
                 )
    
    task_tmp = BashOperator(
            task_id="to.tmp",
            bash_command="""
                echo "to tmp"
                CSV_FILE=~/data/csv/{{logical_date.strftime("%y%m%d")}}/csv.csv
                bash {{var.value.SH_HOME}}/csv2mysql.sh $CSV_FILE {{ds}}

            """
            )
    
    task_base = BashOperator(
            task_id="to.base",
            bash_command="""
                echo "to base"
                bash {{var.value.SH_HOME}}/tmp2base.sh {{ds}}
            """
            )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                figlet "make.done.start"

                DONE_PATH={{var.value.IMPORT_DONE_PATH}}/{{logical_date.strftime("%y%m%d")}}
                mkdir -p $DONE_PATH
                echo "IMPORT_DONE_PATH=$DONE_PATH"
                touch $DONE_PATH/_DONE

                figlet "make.done.end"
            """
            )

    task_err = BashOperator(
            task_id="report.err",
            bash_command="""
                echo "report err"
            """,
            trigger_rule="one_failed"
                )

    
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end',trigger_rule="all_done")

    task_start >> task_check
    task_check >> task_csv >> task_create_tbl >> task_tmp
    task_tmp >> task_base
    task_base >> task_done >> task_end

    task_check >> task_err >> task_end


