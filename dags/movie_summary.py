from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os
from pprint import pprint as pp
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
     'summary_movie',
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
    tags=['summary'],
) as dag:
    
    REQUIREMENTS=[
                "git+https://github.com/hamsunwoo/movie.git@0.3/api"
                ]

    def gen_empty(*ids):
        tasks = []

        for id in ids:
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks) #(t, )

    def gen_vpython(**kw):
        id = kw['id']
        func_o = kw['func_obj']
        op_kw = kw['op_kwargs']

        #task = PythonVirtualenvOperator(
        task = PythonOperator(
        task_id=id,
        python_callable=func_o,
        #requirements=REQUIREMENTS,
        #system_site_packages=False,
        op_kwargs=op_kw
        )
        return task

    def pro_data(**params):
        print("@" * 33)
        print(params['task_name'])
        pp(params)
        print("@" * 33)

    def pro_data2(task_name, **params):
        print("@" * 33)
        print(task_name)
        pp(params)
        if "task_name" in params.keys():
            print("========= 있음")
        else:
            print("========= 없음")
        print("@" * 33)

    def pro_data3(task_name):
        print("@" * 33)
        print(task_name)
        print("@" * 33)

    def pro_data4(task_name, ds_nodash, **kwargs):
        print("@" * 33)
        print(task_name)
        print(ds_nodash)
        print(kwargs)
        print("@" * 33)

    start, end = gen_empty('start', 'end')
    #tasks = gen_empty('start', 'end')
    #start = tasks[0]

    apply_type = gen_vpython(
            id = "apply.type",
            func_obj = pro_data,
            op_kwargs={"task_name": {"task_name": "apply_type!!!"}}
            )

    merge_df = gen_vpython(
            id = "merge.df",
            func_obj = pro_data2,
            op_kwargs={"task_name": {"task_name": "merge_df!!!"}}
            )

    de_dup = gen_vpython(
            id = "de.dup",
            func_obj = pro_data,
            op_kwargs={"task_name": {"task_name": "de_dup!!!"}}
            )

    summary_df = gen_vpython(
            id = "summary.df",
            func_obj = pro_data2,
            op_kwargs={"task_name": {"task_name": "summary!!!"}}
            )

    start >> apply_type >> merge_df >> de_dup >> summary_df >> end

