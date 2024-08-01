from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import os
import producer
import consumer

# Chemin vers le dossier à surveiller
WATCH_FOLDER = '/home/sevan/Documents/IA/Data Engineering/training/course_kafka/airflow_kafka/airflow/dags/data'
# Nom du fichier à surveiller (peut inclure des jokers comme *.csv)
FILE_PATTERN = '*.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_streaming_with_file_watch',
    default_args=default_args,
    description='A Kafka streaming DAG triggered by new files in a folder',
    schedule_interval=timedelta(days=1),
)

# Capteur pour surveiller la présence de nouveaux fichiers dans le dossier
file_sensor = FileSensor(
    task_id='watch_for_new_file',
    poke_interval=10,  # Intervalle de vérification en secondes
    filepath=os.path.join(WATCH_FOLDER, FILE_PATTERN),
    mode='poke',  # Peut être 'poke' ou 'reschedule'
    timeout=600,  # Timeout total en secondes
    dag=dag,
)

produce_task = PythonOperator(
    task_id='produce_messages',
    python_callable=producer.produce_messages,
    dag=dag,
)

consume_task = PythonOperator(
    task_id='consume_messages',
    python_callable=consumer.consume_messages,
    dag=dag,
)

file_sensor >> produce_task >> consume_task
