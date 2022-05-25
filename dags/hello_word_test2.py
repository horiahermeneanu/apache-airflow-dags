#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
                'owner': 'Horia',
                'start_date': datetime(2022, 3, 4),
                'retries': 3,
                'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
hello_world_dag2 = DAG('hello_world_dag2',
                default_args=default_args,
                description='Hello World DAG2',
                schedule_interval='* * * * *',
                catchup=False,
                tags=['example2, helloworld2']
)

def print_hello2():
                return 'Hello World2!'

# Creating first task
start_task2 = DummyOperator(task_id='start_task2', dag=hello_world_dag2)

# Creating second task
hello_world_task2 = PythonOperator(task_id='hello_world_task2', python_callable=print_hello2, dag=hello_world_dag2)

# Creating third task
end_task2 = DummyOperator(task_id='end_task2', dag=hello_world_dag2)

# Set the order of execution of tasks.
start_task2 >> hello_world_task2 >> end_task2
