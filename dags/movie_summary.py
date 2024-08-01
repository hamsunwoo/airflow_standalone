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
     'summery_movie',
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
    tags=['summery'],
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


    apply_type = EmptyOperator(
            task_id="apply.type"
            )
    
    merge_df = EmptyOperator(
            task_id="merge.df"
            )

    de_dup = EmptyOperator(
                task_id='de.dup'
             )

    summery_df = EmptyOperator(
                task_id='summery.df'
                 )
        

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')

    task_start >> apply_type >> merge_df
    merge_df >> de_dup >> summery_df

