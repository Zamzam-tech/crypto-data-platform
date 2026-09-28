from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')
from ingestion.fear_greed_ingestion import fetch_fear_greed, load_fear_greed_to_postgres, load_fear_greed_to_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['arafatzamzam858@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='fear_greed_pipeline',
    schedule=timedelta(hours=24),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args
) as dag:

    fetch_fg = PythonOperator(
        task_id='fetch_fear_greed',
        python_callable=fetch_fear_greed
    )

    load_fg_to_postgres = PythonOperator(
        task_id='load_fear_greed_to_postgres',
        python_callable=load_fear_greed_to_postgres
    )

    load_bigquery_task = PythonOperator(
        task_id='load_fear_greed_to_bigquery',
        python_callable=load_fear_greed_to_bigquery
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt/crypto_pipeline && dbt run --profiles-dir /opt/airflow/dbt_profile'
    )

    test_dbt = BashOperator(
        task_id='test_dbt',
        bash_command='cd /opt/airflow/dbt/crypto_pipeline && dbt test --profiles-dir /opt/airflow/dbt_profile'
    )

    fetch_fg >> load_fg_to_postgres >> load_bigquery_task >> run_dbt >> test_dbt
