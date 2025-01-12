from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

dag = DAG(
    'dbt_transformation_pipeline',
    default_args=default_args,
    description='A DAG to run dbt models in order: bronze -> silver -> gold',
    schedule_interval='@daily',
    catchup=False
)

# Define the dbt run commands for each layer
dbt_run_bronze = BashOperator(
    task_id='dbt_run_bronze',
    bash_command='dbt run --models tag:bronze',
    dag=dag
)

dbt_run_silver = BashOperator(
    task_id='dbt_run_silver',
    bash_command='dbt run --models tag:silver',
    dag=dag
)

dbt_run_gold = BashOperator(
    task_id='dbt_run_gold',
    bash_command='dbt run --models tag:gold',
    dag=dag
)

# Optional: Add dbt test tasks
dbt_test_bronze = BashOperator(
    task_id='dbt_test_bronze',
    bash_command='dbt test --models tag:bronze',
    dag=dag
)

dbt_test_silver = BashOperator(
    task_id='dbt_test_silver',
    bash_command='dbt test --models tag:silver',
    dag=dag
)

dbt_test_gold = BashOperator(
    task_id='dbt_test_gold',
    bash_command='dbt test --models tag:gold',
    dag=dag
)

# Set up dependencies
dbt_run_bronze >> dbt_test_bronze >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold
