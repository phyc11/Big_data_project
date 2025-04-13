from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json
from kafka import KafkaProducer
import time
import uuid
default_args = {
    'owner': 'BigDataProject',
    'start_date': days_ago(0),
}

LOCATIONS = [
    {"name": "Ha Noi", "lat": 21.0285, "lon": 105.8542},
    {"name": "Ho Chi Minh", "lat": 10.8231, "lon": 106.6297},
    {"name": "Da Nang", "lat": 16.0544, "lon": 108.2022},
    {"name": "Hai Phong", "lat": 20.8449, "lon": 106.6881},
    {"name": "Can Tho", "lat": 10.0452, "lon": 105.7469},
    {"name": "An Giang", "lat": 10.5218, "lon": 105.1258},
    {"name": "Ba Ria Vung Tau", "lat": 10.5417, "lon": 107.2530},
    {"name": "Bac Giang", "lat": 21.2731, "lon": 106.1947},
    {"name": "Bac Kan", "lat": 22.1477, "lon": 105.8349},
    {"name": "Bac Lieu", "lat": 9.2940, "lon": 105.7216},
    {"name": "Bac Ninh", "lat": 21.1861, "lon": 106.0763},
    {"name": "Ben Tre", "lat": 10.2433, "lon": 106.3756},
    {"name": "Binh Dinh", "lat": 13.7827, "lon": 109.2239},
    {"name": "Binh Duong", "lat": 11.3254, "lon": 106.7798},
    {"name": "Binh Phuoc", "lat": 11.7514, "lon": 106.7233},
    {"name": "Binh Thuan", "lat": 10.9336, "lon": 108.1428},
    {"name": "Ca Mau", "lat": 9.1527, "lon": 105.1964},
    {"name": "Cao Bang", "lat": 22.6667, "lon": 106.2500},
    {"name": "Dak Lak", "lat": 12.6667, "lon": 108.0500},
    {"name": "Dak Nong", "lat": 12.0045, "lon": 107.6874},
    {"name": "Dien Bien", "lat": 21.3906, "lon": 103.0324},
    {"name": "Dong Nai", "lat": 11.0686, "lon": 107.1676},
    {"name": "Dong Thap", "lat": 10.4938, "lon": 105.6882},
    {"name": "Gia Lai", "lat": 13.9833, "lon": 108.0000},
    {"name": "Ha Giang", "lat": 22.8333, "lon": 105.0000},
    {"name": "Ha Nam", "lat": 20.5464, "lon": 105.9219},
    {"name": "Ha Tinh", "lat": 18.3333, "lon": 105.9000},
    {"name": "Hai Duong", "lat": 20.9400, "lon": 106.3319},
    {"name": "Hau Giang", "lat": 9.7579, "lon": 105.6401},
    {"name": "Hoa Binh", "lat": 20.8133, "lon": 105.3383},
    {"name": "Hung Yen", "lat": 20.6464, "lon": 106.0511},
    {"name": "Khanh Hoa", "lat": 12.2500, "lon": 109.0000},
    {"name": "Kien Giang", "lat": 10.0000, "lon": 105.1667},
    {"name": "Kon Tum", "lat": 14.3500, "lon": 108.0000},
    {"name": "Lai Chau", "lat": 22.3964, "lon": 103.4595},
    {"name": "Lam Dong", "lat": 11.9465, "lon": 108.4419},
    {"name": "Lang Son", "lat": 21.8530, "lon": 106.7613},
    {"name": "Lao Cai", "lat": 22.4833, "lon": 103.9667},
    {"name": "Long An", "lat": 10.5414, "lon": 106.4131},
    {"name": "Nam Dinh", "lat": 20.4200, "lon": 106.1683},
    {"name": "Nghe An", "lat": 19.2345, "lon": 104.9200},
    {"name": "Ninh Binh", "lat": 20.2544, "lon": 105.9753},
    {"name": "Ninh Thuan", "lat": 11.6739, "lon": 108.8628},
    {"name": "Phu Tho", "lat": 21.3227, "lon": 105.1675},
    {"name": "Phu Yen", "lat": 13.0882, "lon": 109.0927},
    {"name": "Quang Binh", "lat": 17.5000, "lon": 106.3333},
    {"name": "Quang Nam", "lat": 15.5394, "lon": 108.0191},
    {"name": "Quang Ngai", "lat": 15.1201, "lon": 108.7964},
    {"name": "Quang Ninh", "lat": 21.0569, "lon": 107.2968},
    {"name": "Quang Tri", "lat": 16.7500, "lon": 107.0000},
    {"name": "Soc Trang", "lat": 9.6042, "lon": 105.9799},
    {"name": "Son La", "lat": 21.1667, "lon": 104.0000},
    {"name": "Tay Ninh", "lat": 11.3230, "lon": 106.1483},
    {"name": "Thai Binh", "lat": 20.4500, "lon": 106.3333},
    {"name": "Thai Nguyen", "lat": 21.5667, "lon": 105.8250},
    {"name": "Thanh Hoa", "lat": 19.8000, "lon": 105.7667},
    {"name": "Thua Thien Hue", "lat": 16.4633, "lon": 107.5856},
    {"name": "Tien Giang", "lat": 10.4544, "lon": 106.3421},
    {"name": "Tra Vinh", "lat": 9.9513, "lon": 106.3346},
    {"name": "Tuyen Quang", "lat": 21.8233, "lon": 105.2181},
    {"name": "Vinh Long", "lat": 10.2537, "lon": 105.9722},
    {"name": "Vinh Phuc", "lat": 21.3608, "lon": 105.5474},
    {"name": "Yen Bai", "lat": 21.7167, "lon": 104.9000}
]

def get_data(lat, lon):
    res = requests.get(f"http://api.weatherapi.com/v1/current.json?key=93ce097640364339aa0135217252503&q={lat},{lon}&aqi=no")
    res = res.json()
    return res

def format_data(res, location):
    data = {}
    data['location'] = location
    data['country'] = res['location']['country']
    data['time'] = res['location']['localtime']
    data['temp_c'] = res['current']['temp_c']
    data['wind_kph'] = res['current']['wind_kph']
    data['humidity'] = res['current']['humidity']
    data['pressure_mb'] = res['current']['pressure_mb']
    data['precip_mm'] = res['current']['precip_mm']
    data['cloud'] = res['current']['cloud']
    data['visibility'] = res['current']['vis_km']
    data['uv'] = res['current']['uv']
    data['condition'] = res['current']['condition']['text']
    return data


def streaming_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    for location in LOCATIONS:
        res = get_data(location['lat'], location['lon'])
        data = format_data(res,location['name'])
        producer.send('weather_forecast', json.dumps(data).encode('utf-8'))
        #print(json.dumps(data, indent=3))
        time.sleep(2)

    # res = get_data()
    # data = format_data(res)
    # producer.send('weather_forecast', json.dumps(data).encode('utf-8'))
    # print(json.dumps(data, indent=3))

dag = DAG(
    'weather_collected',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

streaming_task = PythonOperator(
    task_id='streaming_weather_data',
    python_callable=streaming_data,
    dag=dag,
)

streaming_task