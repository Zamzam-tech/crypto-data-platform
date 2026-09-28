from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/mnt/c/Users/yasab/OneDrive/Desktop/Lean Data pipleine')
from ingestion.crypto_ingestion import  ingest_crypto_data, load_postgres_to_bigquery


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['arafatzamzam858@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='crypto_pipeline',
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args
) as dag:
    
    ingest_task = PythonOperator(
    task_id='ingest_crypto_data',
    python_callable=ingest_crypto_data
    )

    load_bigquery_task = PythonOperator(
        task_id='load_postgres_to_bigquery',
        python_callable=load_postgres_to_bigquery
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        # Added the --select flag to target your incremental crypto models specifically
        bash_command='cd /opt/airflow/dbt/crypto_pipeline && dbt run --profiles-dir /opt/airflow/dbt_profile'
    )

    test_dbt = BashOperator(
        task_id='test_dbt',
        # Matches the selection so you only test what just ran
        bash_command='cd /opt/airflow/dbt/crypto_pipeline && dbt test --profiles-dir /opt/airflow/dbt_profile'
    )

    ingest_task >> load_bigquery_task >> run_dbt >> test_dbt
