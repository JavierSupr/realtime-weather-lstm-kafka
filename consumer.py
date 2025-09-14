from kafka import KafkaConsumer
import psycopg2
import json
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = conn.cursor()

def consumer():
    consumer = KafkaConsumer(
        'weather-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )
    return consumer

def save_to_db(weather, ts):
    sql = """
        INSERT INTO weather_data (weather, ts)
        VALUES (%s, %s)
        ON CONFLICT (ts) DO NOTHING
    """
    cursor.execute(sql, (weather, ts))
    conn.commit()

data = consumer()
for message in data:
    print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
    print(f"Key: {message.key}, Value: {message.value}\n")

    try:
        payload = json.loads(message.value)
        weather = payload.get("weather")
        ts = payload.get("timestamp")

        save_to_db(weather, ts)

    except Exception as e:
        print("Error parsing/saving message:", e)