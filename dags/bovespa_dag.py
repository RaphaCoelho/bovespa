
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.bovespa import main


args = {
    'owner':'Raphael Coelho',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='bovespa_dag',
    default_args=args,
    schedule_interval='@daily'
)

with dag:
    bovespa_dag = PythonOperator(
        task_id='bovespa_script',
        python_callable=main,
    )
