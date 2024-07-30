from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator
        )

os.environ['LC_ALL'] = 'C'

with DAG(
     'movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:

    def get_data(ds_nodash):
        from  mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))
   

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        
        df = apply_type2df(load_dt=ds_nodash)
        
        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)
        
        #개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt':'sum'}).reset_index()
        print(sum_df)

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt/load_dt={ds_nodash}")
        if os.path.exists(path):
            #return "rm_dir" #task_id
            return rm_dir.task_id
        else:
            return "get.data", "echo.task"


    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_func,
            trigger_rule='all_success'
            )
       
    task_get_data = PythonVirtualenvOperator(
            task_id="get.data",
            python_callable=get_data, #함수이름
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False,
            trigger_rule='all_done',
            #venv_cache_path="tmp2/airflow_venv/get_data" #사용한 캐시경로 저장
            )

    save_data = PythonVirtualenvOperator(
            task_id="save.data",
            python_callable=save_data,
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False,
            trigger_rule='one_success',
            #venv_cache_path="tmp2/airflow_venv/get_data"
            )
    
    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt/load_dt={{ ds_nodash }}'
         )

    echo_task = BashOperator(
                task_id='echo.task',
                bash_command="echo 'task'",
                trigger_rule='all_success'
                )
        
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')
    join_task = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done")
    
    task_start >> branch_op
    task_start >> join_task >> save_data

    branch_op >> rm_dir >> task_get_data
    branch_op >> echo_task >> save_data
    branch_op >> task_get_data

    task_get_data >> save_data >> task_end
