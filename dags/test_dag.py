
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.hello_world import hello


args = {
    'owner':'Raphael Coelho',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='test_dag',
    default_args=args,
    schedule_interval='@daily'
)

with dag:
    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello,
    )
