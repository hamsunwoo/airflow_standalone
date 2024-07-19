from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    #first-process 
    core = DummyOperator(task_id='pr_crawling')
    store_urls = DummyOperator(task_id='store_urls')
    dt = DummyOperator(task_id='date')
    title = DummyOperator(task_id='title')
    content = DummyOperator(task_id='content')
    url = DummyOperator(task_id='url')
    impression = DummyOperator(task_id='impression')
    click = DummyOperator(task_id='click')

    #second-process
    clustering = DummyOperator(task_id='clustering')
    noun = DummyOperator(task_id='noun')
    s_character = DummyOperator(task_id='s_character')
    reporter_info = DummyOperator(task_id='reporter_info')
    images = DummyOperator(task_id='images')

    #third-process
    category = DummyOperator(task_id='category')
    date_desc = DummyOperator(task_id='date_desc')

    #end
    end_to_user = DummyOperator(task_id='end_to_user')

    t1 >> core >> store_urls
    store_urls >> dt
    store_urls >> title
    store_urls >> content
    store_urls >> url
    store_urls >> impression
    store_urls >> click

    [dt, title, content, url, impression, click] >> clustering
    clustering >> noun
    clustering >> s_character
    clustering >> reporter_info
    clustering >> images

    [noun, s_character, reporter_info, images] >> category
    category >> date_desc
    
    date_desc >> end_to_user




