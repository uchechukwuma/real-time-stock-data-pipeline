from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kafka_producer import send_to_kafka
from kafka_consumer import consume_data

default_args = {
    'owner': 'Uchechukwu Obi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'kafka_pipeline_dag',
    default_args=default_args,
    description='A DAG to manage Kafka producer and consumer pipeline for stock data',
    schedule_interval='*/2 * * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    concurrency=10,
)

producer_task = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=send_to_kafka,
    dag=dag,
)

consumer_task = PythonOperator(
    task_id='run_kafka_consumer',
    python_callable=consume_data,
    dag=dag,
)

producer_task >> consumer_task
