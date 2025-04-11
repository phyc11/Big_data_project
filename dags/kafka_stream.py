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
    {"name": "HaNoi", "lat": 21.0285, "lon": 105.8542},
    {"name": "HoChiMinh", "lat": 10.8231, "lon": 106.6297},
    {"name": "DaNang", "lat": 16.0544, "lon": 108.2022},
    {"name": "HaiPhong", "lat": 20.8449, "lon": 106.6881},
    {"name": "CanTho", "lat": 10.0452, "lon": 105.7469},
    {"name": "AnGiang", "lat": 10.5218, "lon": 105.1258},
    {"name": "BaRia_VungTau", "lat": 10.5417, "lon": 107.2530},
    {"name": "BacGiang", "lat": 21.2731, "lon": 106.1947},
    {"name": "BacKan", "lat": 22.1477, "lon": 105.8349},
    {"name": "BacLieu", "lat": 9.2940, "lon": 105.7216},
    {"name": "BacNinh", "lat": 21.1861, "lon": 106.0763},
    {"name": "BenTre", "lat": 10.2433, "lon": 106.3756},
    {"name": "BinhDinh", "lat": 13.7827, "lon": 109.2239},
    {"name": "BinhDuong", "lat": 11.3254, "lon": 106.7798},
    {"name": "BinhPhuoc", "lat": 11.7514, "lon": 106.7233},
    {"name": "BinhThuan", "lat": 10.9336, "lon": 108.1428},
    {"name": "CaMau", "lat": 9.1527, "lon": 105.1964},
    {"name": "CaoBang", "lat": 22.6667, "lon": 106.2500},
    {"name": "DakLak", "lat": 12.6667, "lon": 108.0500},
    {"name": "DakNong", "lat": 12.0045, "lon": 107.6874},
    {"name": "DienBien", "lat": 21.3906, "lon": 103.0324},
    {"name": "DongNai", "lat": 11.0686, "lon": 107.1676},
    {"name": "DongThap", "lat": 10.4938, "lon": 105.6882},
    {"name": "GiaLai", "lat": 13.9833, "lon": 108.0000},
    {"name": "HaGiang", "lat": 22.8333, "lon": 105.0000},
    {"name": "HaNam", "lat": 20.5464, "lon": 105.9219},
    {"name": "HaTinh", "lat": 18.3333, "lon": 105.9000},
    {"name": "HaiDuong", "lat": 20.9400, "lon": 106.3319},
    {"name": "HauGiang", "lat": 9.7579, "lon": 105.6401},
    {"name": "HoaBinh", "lat": 20.8133, "lon": 105.3383},
    {"name": "HungYen", "lat": 20.6464, "lon": 106.0511},
    {"name": "KhanhHoa", "lat": 12.2500, "lon": 109.0000},
    {"name": "KienGiang", "lat": 10.0000, "lon": 105.1667},
    {"name": "KonTum", "lat": 14.3500, "lon": 108.0000},
    {"name": "LaiChau", "lat": 22.3964, "lon": 103.4595},
    {"name": "LamDong", "lat": 11.9465, "lon": 108.4419},
    {"name": "LangSon", "lat": 21.8530, "lon": 106.7613},
    {"name": "LaoCai", "lat": 22.4833, "lon": 103.9667},
    {"name": "LongAn", "lat": 10.5414, "lon": 106.4131},
    {"name": "NamDinh", "lat": 20.4200, "lon": 106.1683},
    {"name": "NgheAn", "lat": 19.2345, "lon": 104.9200},
    {"name": "NinhBinh", "lat": 20.2544, "lon": 105.9753},
    {"name": "NinhThuan", "lat": 11.6739, "lon": 108.8628},
    {"name": "PhuTho", "lat": 21.3227, "lon": 105.1675},
    {"name": "PhuYen", "lat": 13.0882, "lon": 109.0927},
    {"name": "QuangBinh", "lat": 17.5000, "lon": 106.3333},
    {"name": "QuangNam", "lat": 15.5394, "lon": 108.0191},
    {"name": "QuangNgai", "lat": 15.1201, "lon": 108.7964},
    {"name": "QuangNinh", "lat": 21.0569, "lon": 107.2968},
    {"name": "QuangTri", "lat": 16.7500, "lon": 107.0000},
    {"name": "SocTrang", "lat": 9.6042, "lon": 105.9799},
    {"name": "SonLa", "lat": 21.1667, "lon": 104.0000},
    {"name": "TayNinh", "lat": 11.3230, "lon": 106.1483},
    {"name": "ThaiBinh", "lat": 20.4500, "lon": 106.3333},
    {"name": "ThaiNguyen", "lat": 21.5667, "lon": 105.8250},
    {"name": "ThanhHoa", "lat": 19.8000, "lon": 105.7667},
    {"name": "ThuaThienHue", "lat": 16.4633, "lon": 107.5856},
    {"name": "TienGiang", "lat": 10.4544, "lon": 106.3421},
    {"name": "TraVinh", "lat": 9.9513, "lon": 106.3346},
    {"name": "TuyenQuang", "lat": 21.8233, "lon": 105.2181},
    {"name": "VinhLong", "lat": 10.2537, "lon": 105.9722},
    {"name": "VinhPhuc", "lat": 21.3608, "lon": 105.5474},
    {"name": "YenBai", "lat": 21.7167, "lon": 104.9000}
]

def get_data(lat, lon):
    res = requests.get(f"http://api.weatherapi.com/v1/current.json?key=93ce097640364339aa0135217252503&q={lat},{lon}&aqi=no")
    res = res.json()
    return res

def format_data(res):
    data = {}
    data['location'] = res['location']['name']
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
        data = format_data(res)
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