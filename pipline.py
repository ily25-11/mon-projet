from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_pipeline',
    default_args=default_args,
    description='A simple DAG pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def task_1():
    print("Task 1 executed")

def task_2():
    print("Task 2 executed")

def task_3():
    print("Task 3 executed")

t1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1,
    dag=dag,
)

t2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2,
    dag=dag,
)

t3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3,
    dag=dag,
)

t1 >> t2 >> t3