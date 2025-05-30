from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidatorModel
import pandas as pd
from datetime import datetime, timedelta
import subprocess

# Khởi tạo SparkSession với ES connector
spark = SparkSession.builder \
    .appName("WeatherForecast") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.4") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Load mô hình từ HDFS
loaded_model = CrossValidatorModel.load("hdfs://namenode:9000/model/rf_model_hn")

# Tìm file parquet mới nhất trong HDFS
def get_latest_file_hdfs(hdfs_path):
    try:
        result = subprocess.run(['hdfs', 'dfs', '-ls', hdfs_path], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error listing HDFS directory: {result.stderr}")
            return None
        lines = result.stdout.strip().split('\n')
        parquet_files = [(line.split()[5] + ' ' + line.split()[6], line.split()[7]) 
                         for line in lines if '.parquet' in line]
        if not parquet_files:
            return None
        parquet_files.sort(key=lambda x: x[0], reverse=True)
        return parquet_files[0][1]
    except Exception as e:
        print(f"Error: {e}")
        return None

# Đọc dữ liệu từ file parquet mới nhất
hdfs_dir = "hdfs://namenode:9000/data_from_API/weather_API_data/location=Hanoi,VN/"
latest_file = get_latest_file_hdfs(hdfs_dir)

if latest_file:
    print(f"Đang đọc dữ liệu từ: {latest_file}")
    df_test = spark.read.parquet(latest_file)
else:
    print("Không tìm thấy file mới nhất, đang đọc fallback...")
    from pyspark.sql.functions import input_file_name
    df_all = spark.read.parquet(hdfs_dir).withColumn("filename", input_file_name())
    latest_file_spark = df_all.select("filename").distinct().orderBy("filename", ascending=False).first()[0]
    df_test = spark.read.parquet(latest_file_spark)

# Xử lý dữ liệu: loại bỏ cột không cần thiết
df_test = df_test.drop("date", "Time", "actual_hour")
df_test = df_test.toPandas()

# Mapping Weather thành số
weather_mapping = {}
for w in ['Sunny', 'Clear', 'Partly cloudy', 'Partially cloudy']:
    weather_mapping[w] = 0
for w in ['Overcast', 'Cloudy', 'Patchy rain possible', 'Rain, Overcast', 'Rain, Partially cloudy', 'Light drizzle', 'Light rain shower', 'Patchy light rain with thunder']:
    weather_mapping[w] = 1
for w in ['Heavy rain at times', 'Moderate or heavy rain shower', 'Moderate rain at times', 'Moderate rain']:
    weather_mapping[w] = 2

df_test['Weather'] = df_test['Weather'].map(weather_mapping)

# Ép kiểu dữ liệu cho các cột số
df_test = df_test.astype({
    'temp': 'float64',
    'rain': 'float64',
    'cloud': 'float64',
    'pressure': 'float64',
    'windspeed': 'float64',
    'Gust': 'float64',
    'Weather': 'int64'
})

# Đổi tên cột để phù hợp với model
df_test = df_test.rename(columns={
    'temp': 'Temp(°c)',
    'rain': 'Rain(nmm)',
    'cloud': 'Cloud(%)',
    'pressure': 'Pressure(mb)',
    'windspeed': 'Wind(km/h)',
    'Gust': 'Gust(km/h)'
})

# Tạo features cho prediction
def predict_next_weather(df_test):
    df = df_test.copy()
    print("Dữ liệu đầu vào (8 dòng):")
    print(df.tail(8))
    
    prediction_row = {}
    latest_row = df.iloc[-1]
    
    for col in ['Temp(°c)', 'Rain(nmm)', 'Cloud(%)', 'Pressure(mb)', 'Wind(km/h)', 'Gust(km/h)', 'Weather']:
        prediction_row[col] = latest_row[col]
        
    for lag in range(1, 9):
        row_idx = len(df) - lag - 1
        if row_idx >= 0:
            for col in ['Temp(°c)', 'Rain(nmm)', 'Cloud(%)', 'Pressure(mb)', 'Wind(km/h)', 'Gust(km/h)']:
                prediction_row[f'{col.split("(")[0]}_t-{lag}'] = df.iloc[row_idx][col]
            prediction_row[f'Weather_t-{lag}'] = df.iloc[row_idx]['Weather']
    
    return pd.DataFrame([prediction_row])

print("=== DỰ ĐOÁN THỜI TIẾT CHO THỜI ĐIỂM TIẾP THEO ===")
df_prediction_ready = predict_next_weather(df_test)
df_test_spark = spark.createDataFrame(df_prediction_ready)

# Dự đoán bằng model
predictions = loaded_model.transform(df_test_spark)
prediction_result = predictions.select("prediction").collect()[0][0]

weather_text = {
    0: "Thời tiết đẹp - Nắng/ít mây",
    1: "Thời tiết trung bình - Nhiều mây/mưa nhẹ",
    2: "Thời tiết xấu - Mưa lớn"
}.get(int(prediction_result), "Unknown")

# Chuẩn bị ghi vào Elasticsearch
result = spark.createDataFrame([{
    'prediction_id': f"pred_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    'location': 'Hanoi,VN',
    'timestamp': datetime.now().isoformat() + timedelta(hours=3).isoformat(),
    'prediction_value': int(prediction_result),
    'prediction_text': weather_text,
    'temperature': float(df_prediction_ready.iloc[0]['Temp(°c)']),
    'rain': float(df_prediction_ready.iloc[0]['Rain(nmm)']),
    'cloud': float(df_prediction_ready.iloc[0]['Cloud(%)']),
    'pressure': float(df_prediction_ready.iloc[0]['Pressure(mb)']),
    'wind_speed': float(df_prediction_ready.iloc[0]['Wind(km/h)']),
    'gust': float(df_prediction_ready.iloc[0]['Gust(km/h)'])
}])

result.write.format("es") \
    .option("es.resource", f"weather_predictions_{datetime.now().strftime('%Y_%m')}") \
    .option("es.mapping.id", "prediction_id") \
    .mode("append").save()

print("\n=== KẾT QUẢ DỰ ĐOÁN ===")
print(f"Dự đoán thời tiết cho thời điểm tiếp theo: {weather_text}")
print("Đã ghi kết quả vào Elasticsearch")

spark.stop()
