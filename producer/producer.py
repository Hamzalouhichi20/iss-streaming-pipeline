import time
import json 
import requests 
from kafka import KafkaProducer

# Kafka Producer config
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# APIs
ISS_API = "https://api.wheretheiss.at/v1/satellites/25544"
REVERSE_GEO_API = "https://nominatim.openstreetmap.org/reverse"

def get_iss_position():
    try:
        response = requests.get(ISS_API)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def get_country(lat, lon):
    try:
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json'
        }
        headers = {
            'User-Agent': 'iss-tracker'
        }
        response = requests.get(REVERSE_GEO_API, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data.get('address', {}).get('country', 'Unknown')
    except:
        pass
    return 'Unknown'

while True:
    iss_data = get_iss_position()
    if iss_data:
        latitude = iss_data.get("latitude")
        longitude = iss_data.get("longitude")
        country = get_country(latitude, longitude)

        enriched_data = {
            "timestamp": iss_data.get("timestamp"),
            "latitude": latitude,
            "longitude": longitude,
            "altitude": iss_data.get("altitude"),
            "velocity": iss_data.get("velocity"),
            "country": country
        }

        producer.send("iss_data", enriched_data)
        print("Envoyé à Kafka:", enriched_data)

    time.sleep(1)
