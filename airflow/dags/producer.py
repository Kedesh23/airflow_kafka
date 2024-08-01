from kafka import KafkaProducer
import pandas as pd


KAFKA_TOPIC = 'obesity'
KAFKA_BROKER_URL = 'localhost:9092'
CSV_FILE_PATH = '/home/sevan/Documents/IA/Data Engineering/training/course_kafka/airflow_kafka/airflow/dags/data/obesity.csv'


def clean_and_transform(df):
    df.dropna(inplace=True)
    return df


def produce_messages():
    df = pd.read_csv(CSV_FILE_PATH)
    cleaned_df = clean_and_transform(df)
    messages = cleaned_df.to_dict(orient='records')
    producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER_URL)

    for message in messages:
        producer.send(KAFKA_TOPIC, value=str(message).encode('utf-8'))
    producer.flush()



if __name__ == "__main__":
    produce_messages()