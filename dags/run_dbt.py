from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'dbt_bigquery_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --profiles-dir /usr/local/airflow/dbt_project --project-dir /usr/local/airflow/dbt_project',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --profiles-dir /usr/local/airflow/dbt_project --project-dir /usr/local/airflow/dbt_project',
    )

    dbt_run >> dbt_test