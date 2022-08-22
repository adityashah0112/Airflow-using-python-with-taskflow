from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.decorators import dag, task

default_args = {
    'owner':        'Aditya Shah',
    'retries':      5,
    'retry_delay':  timedelta(minutes=2)
}


@dag(
    dag_id              = 'dag_using_taskflow_V02',
    default_args        = default_args,
    description         = 'This is the first attempt at a DAG.',
    schedule_interval   = '* * * * *',
    start_date          = datetime(year=2022, month=8, day=13),
    catchup             = False
)

def hello_world_etl():

    @task(multiple_outputs = True)
    def get_name():
        return {
            'first_name':   'Aditya',
            'last_name':    'Shah'            
        }

    @task()
    def get_age():
        return 23
    
    @task()
    def greet(first_name, last_name, age):
        print(
            f'Hello World! My name is {first_name} {last_name}. ' +
            f'I am {age} years old!'
        )
    
    name_dict = get_name()
    age = get_age()
    greet(
        first_name = name_dict['first_name'], 
        last_name = name_dict['last_name'],
        age = age)

greet_dag = hello_world_etl()

    