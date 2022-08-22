import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def process_datetime(ti):
    dt = ti.xcom_pull(task_ids=['get_datetime'])
    if not dt:
        raise Exception('Not Timestamp Found')
    
    dt = str(dt[0]).split()
    return {
        'year':     int(dt[-1]),
        'month':    dt[1],
        'date':     int(dt[2]),
        'time':     dt[3],
        'day':      dt[0]
    }

def save_datetime(ti):
    dt_processed = ti.xcom_pull(task_ids=['process_datetime'])
    if not dt_processed:
        raise Exception('No processed datetime value!')

    df = pd.DataFrame(dt_processed)
    
    csv_path = Variable.get('first_dag_csv_path')
    if os.path.exists(csv_path):
        df_header = False
        df_mode = 'a'
    else:
        df_header = True
        df_mode = 'w'

    df.to_csv(csv_path, index=False, header=df_header, mode=df_mode)

default_args = {
    'owner':        'Aditya Shah',
    'retries':      5,
    'retry_delay':  timedelta(minutes=2)
}


with DAG (
    dag_id              = 'first_airflow_dag',
    default_args        = default_args,
    description         = 'This is the first attempt at a DAG.',
    schedule_interval   = '* * * * *',
    start_date          = datetime(year=2022, month=8, day=13),
    catchup             = False
) as dag:
    
    # 1. Get current Datetime
    task_get_date = BashOperator(
        task_id = 'get_datetime',
        bash_command = 'date'
    )

    # 2. Process current datetime
    task_process_datetime = PythonOperator(
        task_id = 'process_datetime',
        python_callable = process_datetime
    )

    # 3. Save processed datetime
    task_save_datetime = PythonOperator(
        task_id = 'save_datetime',
        python_callable = save_datetime
    )

    task_get_date >> task_process_datetime >> task_save_datetime