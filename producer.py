from kafka import KafkaProducer
import json
import requests
import time

URL = "https://api.open-meteo.com/v1/forecast?latitude=-6.2349&longitude=106.9896&hourly=temperature_2m&timezone=Asia%2FBangkok&past_days=1&forecast_days=1"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "weather-data"

while True:
    try:
        r = requests.get(URL)
        #print(f"rrr{r}")
        data = r.json()
        #print(f"data {data}")

        feature = {
            "temperature": data["hourly"]["temperature_2m"]
        }
        print("Data berhasil terkirim!")

        producer.send(topic, feature)
        producer.flush()
        
    except Exception as e:
        print("Error:", e)
    
    time.sleep(5)