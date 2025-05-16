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
import datetime
import pytz 
import pandas as pd
default_args = {
    'owner': 'BigDataProject',
    'start_date': days_ago(0),
}


LOCATIONS = [

    "Hanoi",        # Miền Bắc
    "Haiphong",     # Miền Bắc
    "Nghean",       # Bắc Trung Bộ
    "Danang",       # Trung Trung Bộ
    "Quangngai",    # Nam Trung Bộ
    "Daklak",       # Tây Nguyên
    "Hochiminh",    # Đông Nam Bộ
    "Cantho"     # Đồng bằng sông Cửu Long
     # Đồng bằng sông Cửu Long

]
def get_weather_data(api_key, location, start_date, end_date):
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"
    params = {
        "unitGroup": "metric",
        "include": "hours",
        "key": api_key,
        "contentType": "json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()



def get_timestamps_now(count=8):
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.datetime.now(vietnam_tz)
    timestamps = [now - datetime.timedelta(hours=3 * i) for i in range(count)]
    return timestamps[::-1]
def find_closest_hour_data(data, target_datetime):
    
    target_date = target_datetime.strftime('%Y-%m-%d')
    target_hour = target_datetime.hour
    day_data = None
    for day in data.get('days', []):
        if day['datetime'] == target_date:
            day_data = day
            break
    
    if not day_data:
        return None
    closest_hour_data = None
    min_diff = 24  
    
    for hour_data in day_data.get('hours', []):
        hour = int(hour_data['datetime'].split(':')[0])
        diff = abs(hour - target_hour)
        if diff < min_diff:
            min_diff = diff
            closest_hour_data = hour_data
    
    return {
        "location":data.get("address"),
        "date": f"{target_date}",
        "Time":target_datetime.strftime('%H:%M'),
        "Weather": closest_hour_data.get("conditions"),
        "temp": closest_hour_data.get("temp"),
        "rain": closest_hour_data.get("precip"),
        "cloud":closest_hour_data.get("cloudcover"),
        "pressure":closest_hour_data.get("pressure"),
        "humidity": closest_hour_data.get("humidity"),
        "windspeed": closest_hour_data.get("windspeed"),
        "Gust":closest_hour_data.get("windgust"), 
        "actual_hour": closest_hour_data.get("datetime") ,
        
    }
def get_data(location):
    API_KEY = "WXMXL5WQ42ZSHMM8ZN2AHZ5Q9"  
    API_KEY_2 = "RMX4JCJ9L8PK2AJ57TT4E8CDB"  
    API_KEY_3 ="VUD89TDP3HWWFDGPD9XYQRY57"
    API_KEY_4="53NLGN6GC7M28L4C8BXPDQDHA"
    
    LOCATION = f"{location},VN"
    
    
    timestamps = get_timestamps_now(4)
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.datetime.now(vietnam_tz)
    
    start_date = timestamps[0].strftime('%Y-%m-%d')
    end_date = timestamps[-1].strftime('%Y-%m-%d')
    if start_date == end_date:
        end_date = (timestamps[-1] + datetime.timedelta(days=1)).strftime('%Y-%m-%d') 
    # Lấy dữ liệu từ API
    data = get_weather_data(API_KEY_3, LOCATION, start_date, end_date)
    result = []
    for ts in timestamps:
        hour_data = find_closest_hour_data(data, ts)
        if hour_data:
            result.append(hour_data)
    
    return result



def streaming_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    for location in LOCATIONS:

        res = get_data(location)
        
        producer.send('weather_forecast', json.dumps(res).encode('utf-8'))


        #print(json.dumps(data, indent=3))
        time.sleep(2) 

    

dag = DAG(
    'weather_collected',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
)

streaming_task = PythonOperator(
    task_id='streaming_weather_data',
    python_callable=streaming_data,
    dag=dag,
)

streaming_task
# streaming_data()

