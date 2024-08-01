from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os
from pprint import pprint
import pandas as pd

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
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
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
   
    def func_multi(load_dt, **kwargs):
        from mov.api.call import save2df
        df = save2df(load_dt=load_dt, url_param=kwargs['url_param'])

        for k, v in kwargs['url_param'].items():
            df[k] = v

        p_cols = ['load_dt'] + list(kwargs['url_param'].keys())
        df.to_parquet('~/tmp/test_parquet/load_dt', partition_cols=p_cols)
        print(df.head(5))

    def save_data(ds_nodash):
        from mov.api.call import save2df, apply_type2df
         
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
            return rm_dir.task_id
        else:
            return "get.start", "echo.task"


    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_func
            )
    
    save_data = PythonVirtualenvOperator(
            task_id="save.data",
            python_callable=save_data,
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False,
            trigger_rule='one_success',
            #venv_cache_path="tmp2/airflow_venv/get_data"
            )

    #다양성 영화 유무 
    multi_y = PythonVirtualenvOperator(
            task_id='multi.y',
            python_callable=func_multi,
            op_args=["{{ds_nodash}}"],
            op_kwargs={
                'url_param': {"multiMovieYn": "Y"}
                       },
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False
            )

    multi_n = PythonVirtualenvOperator(
            task_id='multi.n',
            python_callable=func_multi,
            op_args=["{{ds_nodash}}"],
            op_kwargs={
                'url_param': {"multiMovieYn": "N"}
                    },
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False
            )

    #한국외국영화
    nation_k = PythonVirtualenvOperator(
            task_id='nation.k',
            python_callable=func_multi,
            op_args=["{{ds_nodash}}"],
            op_kwargs={
                'url_param': {"repNationCd": "K"}
                    },
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False
            )

    nation_f = PythonVirtualenvOperator(
            task_id='nation.f',
            python_callable=func_multi,
            op_args=["{{ds_nodash}}"],
            op_kwargs={
                'url_param': {"repNationCd": "F"}
                    },
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False
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

    get_start = EmptyOperator(task_id='get.start', trigger_rule='all_done')
    get_end = EmptyOperator(task_id='get.end')

    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"
            )
    
    task_start >> branch_op
    task_start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start 
    branch_op >> get_start
    branch_op >> echo_task

    get_start >> [multi_y, multi_n, nation_k, nation_f] >> get_end

    get_end >> save_data >> task_end
