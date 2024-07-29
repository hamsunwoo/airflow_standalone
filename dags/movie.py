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
        PythonVirtualenvOperator
        )

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

    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)
        from  mov.api.call import get_key, save2df
        key = get_key()
        print(f"MOVIE_API_KEY=>{key}")
        YYYYMMDD = kwargs['ds_nodash'] #20240724
        df = save2df(YYYYMMDD)
        print(df.head(5))

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
            task_id="print_the_context",
            python_callable=print_context 
            )
       
    task_get_data = PythonVirtualenvOperator(
            task_id="get.data",
            python_callable=get_data, #함수이름
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.2/api"],
            system_site_packages=False #위에 패키지만 설치하고 다른 시스템에는 영향안주게
            )
    
    task_save_data = BashOperator(
            task_id="save.data",
            bash_command="""
                echo "save data"
            """    
            )

        
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end',trigger_rule="all_done")

    task_start >> task_get_data >> task_save_data >> task_end
    task_start >> run_this >> task_end
