from datetime import datetime, timedelta
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Uchechukwu Obi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

def check_container_health(container_name):
    """
    Checks if a microservice container is up and running in the Docker network.
    """
    try:
        # Runs a lightweight shell check to see if the container is marked as 'running'
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
            capture_output=True, text=True, check=True
        )
        if "true" not in result.stdout.lower():
            raise ValueError(f"Infrastructure Alert: {container_name} is not running!")
        print(f"Success: {container_name} container status is active and healthy.")
    except Exception as e:
        print(f"Health check failed for {container_name}. Error: {str(e)}")
        raise e

with DAG(
    'kafka_pipeline_dag',
    default_args=default_args,
    description='Infrastructure Supervisor: Monitors active streaming microservices',
    schedule_interval='*/5 * * * *',  # Checked every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    monitor_producer = PythonOperator(
        task_id='monitor_kafka_producer_health',
        python_callable=check_container_health,
        op_kwargs={'container_name': 'real-time-stock-data-pipeline-kafka-producer-1'},
    )

    monitor_consumer = PythonOperator(
        task_id='monitor_kafka_consumer_health',
        python_callable=check_container_health,
        op_kwargs={'container_name': 'real-time-stock-data-pipeline-kafka-consumer-1'},
    )

    # In a supervisor model, we check both core components concurrently
    [monitor_producer, monitor_consumer]