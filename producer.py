from kafka import KafkaProducer
import json
import requests
import time

URL = "https://api.open-meteo.com/v1/forecast?latitude=-6.2349&longitude=106.9896&hourly=temperature_2m&past_days=1&forecast_days=1"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "weather-data"

sent_times = set()

while True:
    r = requests.get(URL)
    data = r.json()
    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]

    for t, temp in zip(times, temps):
        if t not in sent_times:  
            feature = {"timestamp": t, "weather": temp}
            producer.send(topic, feature)
            sent_times.add(t)
            print("Data terkirim:", feature)

    producer.flush()
    time.sleep(3600)
