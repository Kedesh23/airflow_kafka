from kafka import KafkaConsumer
import sqlalchemy as db
import json
import pandas as pd


KAFKA_TOPIC = 'obesity'
KAFKA_BROKER_URL = 'localhost:9092'
url = "postgresql://obesity_owner:d8DrHLv6elEN@ep-falling-night-a5olrbt6.us-east-2.aws.neon.tech/obesity?sslmode=require"
engine = db.create_engine(url)


def consume_messages():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers = KAFKA_BROKER_URL)
    messages = []
    for message in consumer:
        document = json.loads(message.value.docode('utf-8'))
        messages.append(document)

        # Pour la demonstration, nous traitons 20 messages a la fois
        if len(messages) >= 20:
            df = pd.DataFrame(messages)
            df.to_sql('obesity',engine, if_exists='append', index=False)

        if messages:
            df = pd.DataFrame(messages)
            df.to_sql('obesity',engine, if_exists='append', index=False)


if __name__ == "__main__":
    consume_messages()