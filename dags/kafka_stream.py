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

def get_data():
    res = requests.get("http://api.weatherapi.com/v1/current.json?key=93ce097640364339aa0135217252503&q=HaNoi&aqi=no")
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

    res = get_data()
    data = format_data(res)
    producer.send('weather_forecast', json.dumps(data).encode('utf-8'))
    print(json.dumps(data, indent=3))

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