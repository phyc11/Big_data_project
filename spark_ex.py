from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, explode, lit, udf, lead, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from pyspark.sql.functions import to_timestamp, expr, to_date
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("WeatherForecast") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.4") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def check_hdfs_path(path):
    try:
        hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration())
        path_exists = hdfs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
        if path_exists:
            print(f"Path {path} exists in HDFS")
        else:
            print(f"Path {path} does not exist in HDFS")
            try:
                
                hdfs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(path))
                print(f"Created directory {path}")
            except Exception as e:
                print(f"Cannot create directory {path}: {e}")
    except Exception as e:
        print(f"Error checking HDFS path: {e}")

check_hdfs_path("hdfs://namenode:9000/tmp/weather_data/current")
check_hdfs_path("hdfs://namenode:9000/tmp/weather_data/predictions")
check_hdfs_path("hdfs://namenode:9000/tmp/checkpoints")
check_hdfs_path("hdfs://namenode:9000/tmp/checkpoints/predictions")

# Load models từ HDFS
try:
    conditions_model_path = "hdfs://namenode:9000/model/rf_model/weather_model"
    temp_model_path = "hdfs://namenode:9000/model/rf_model/temperature_model"
    cloud_model_path = "hdfs://namenode:9000/model/rf_model/cloud_model"
    
    
    check_hdfs_path(conditions_model_path)
    check_hdfs_path(temp_model_path)
    check_hdfs_path(cloud_model_path)
    
    conditions_model = PipelineModel.load(conditions_model_path)
    temp_model = PipelineModel.load(temp_model_path)
    cloud_model = PipelineModel.load(cloud_model_path)
    
    print("Models loaded successfully")
except Exception as e:
    print(f"Error loading models: {e}")
    
    conditions_model = None
    temp_model = None
    cloud_model = None

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "weather_forecast") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Định nghĩa schema
data_schema = StructType([
    StructField("location", StringType(), True),
    StructField("date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Weather", StringType(), True),
    StructField("temp", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("cloud", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("Gust", DoubleType(), True),
    StructField("actual_hour", StringType(), True)
])

# Parse JSON từ Kafka
df_selected = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ArrayType(data_schema)).alias("data")) \
    .select(explode(col("data")).alias("weather")) \
    .select("weather.*")

# Map conditions weather string thành code
conditions_mapping = {
    "Clear": 0.0,
    "Overcast":1.0,
    "Partially cloudy":1.0,
    "Rain, Partially cloudy":2.0,
    "Rain": 2.0, 
    "Rain, Overcast": 2.0 
}

# Reverse mapping cho việc chuyển đổi dự đoán
condition_mapping_reverse = {
    0.0: "sunny or Clear",
    1.0: "Cloudy or drizzle possible",
    2.0: "Rain"
    
}

# Define UDF để chuyển đổi prediction codes thành condition strings
map_conditions = udf(lambda code: condition_mapping_reverse.get(code, "Unknown"), StringType())
map_weather_to_code = udf(lambda condition: conditions_mapping.get(condition, 0.0), DoubleType())


def check_dataframe_columns(df):
    # print(f"Schema của {name}:")
    # df.printSchema()
    print(f"Số cột: {len(df.columns)}")
    print(f"Tên các cột: {df.columns}")

def process_batch_with_prediction(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty")
        return
    
    print(f"Processing batch {batch_id}")
    
    print("Sample data:")
    batch_df.show(8, truncate=False)
    
    # Chuyển đổi các cột string sang đúng kiểu dữ liệu
    processed_df = batch_df \
        .withColumn("date", to_date(col("date"))) \
        .withColumn("actual_hour", to_timestamp(col("actual_hour"), "HH:mm:ss")) \
        .withColumn("Weather", map_weather_to_code(col("Weather")))
    
    
    processed_df = processed_df.withColumn("hour", F.hour(col("actual_hour")))
    
    
    window_spec = Window.partitionBy("location").orderBy("date", "hour")
    
    with_row_nums = processed_df \
        .withColumn("row_num", F.row_number().over(window_spec))
    
    location_counts = with_row_nums.groupBy("location").count()
    valid_locations = location_counts.filter(col("count") >= 4).select("location")
    
    # Inner join để chỉ giữ lại các địa điểm có đủ dữ liệu
    with_row_nums = with_row_nums.join(valid_locations, "location")
    
    # Với mỗi địa điểm, lấy 4 hàng mới nhất
    location_window = Window.partitionBy("location").orderBy(F.desc("date"), F.desc("hour"))
    recent_data = with_row_nums \
        .withColumn("recency_rank", F.row_number().over(location_window)) \
        .filter(col("recency_rank") <= 4) \
        .orderBy("location", "recency_rank")
    
    
    current_data = recent_data.filter(col("recency_rank") == 1)
    
    # Pivoting dữ liệu lịch sử để thêm vào tính năng
    feature_cols = ["temp", "rain", "cloud", "pressure","windspeed", "Gust"]
    
    
    
    # Kết hợp dữ liệu hiện tại với dữ liệu lịch sử
    for location_df in current_data.select("location").distinct().collect():
        location_name = location_df["location"]
    
        if location_name:
            location_data = recent_data.filter(col("location") == location_name)
        else:
            print("location_name is None. Skipping prediction for this batch.")
            continue

        
        # In thử số hàng để xác nhận có dữ liệu
        row_count = location_data.count()
        print(f"Location {location_name} has {row_count} data points")
        
        if row_count < 4:
            print(f"Skipping location {location_name} - not enough data points")
            continue
        
        # Tạo DataFrame với dữ liệu hiện tại và lịch sử
        rows = location_data.collect()
        
        # Điểm dữ liệu hiện tại (recency_rank = 1)
        current_row = next((r for r in rows if r["recency_rank"] == 1), None)
        if not current_row:
            print(f"Cannot find current data point for location {location_name}")
            continue
            
        # Lấy dữ liệu 3 giờ trước (recency_rank = 2)
        lag1_row = next((r for r in rows if r["recency_rank"] == 2), None)
        
        # Lấy dữ liệu 6 giờ trước (recency_rank = 3)
        lag2_row = next((r for r in rows if r["recency_rank"] == 3), None)
        
        # Lấy dữ liệu 9 giờ trước (recency_rank = 4)
        lag3_row = next((r for r in rows if r["recency_rank"] == 4), None)
        
        if not (lag1_row and lag2_row and lag3_row):
            print(f"Missing historical data for location {location_name}")
            continue
        
        # Tạo một DataFrame mới với dữ liệu hiện tại
        current_df = spark.createDataFrame([current_row])
        
        # Tạo các cột lag cho từng tính năng
        feature_data = []
        for col_name in feature_cols:
            feature_data.extend([
                (col_name, 1, lag1_row[col_name]),
                (col_name, 2, lag2_row[col_name]),
                (col_name, 3, lag3_row[col_name])
            ])
        
        feature_schema = StructType([
            StructField("feature", StringType(), True),
            StructField("lag", IntegerType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        lag_df = spark.createDataFrame(feature_data, schema=feature_schema)
        
        # Tạo DataFrame dự đoán cho địa điểm hiện tại
        prediction_df = current_df
        
        # Thêm các cột lag vào DataFrame
        for feature_row in lag_df.collect():
            feature_name = feature_row["feature"]
            lag_num = feature_row["lag"]
            feature_value = feature_row["value"]
            
            prediction_df = prediction_df.withColumn(
                f"{feature_name}_lag_{lag_num}", 
                lit(feature_value)
            )
        
        # Tạo vectơ tính năng cho dự đoán
        all_features = feature_cols + ["hour"]
        lag_features = [f"{col_name}_lag_{lag}" for col_name in feature_cols for lag in [1, 2, 3]]
        feature_vector_cols = all_features + lag_features
        
        # Kiểm tra tất cả các cột trong DataFrame
        check_dataframe_columns(prediction_df, "prediction_df")
        
        # Kiểm tra xem tất cả các cột đầu vào có trong DataFrame không
        missing_cols = [col for col in feature_vector_cols if col not in prediction_df.columns]
        if missing_cols:
            print(f"Thiếu các cột sau trong prediction_df: {missing_cols}")
            continue
        
        
        
        # Dự đoán nếu các mô hình có sẵn
        if conditions_model and temp_model and cloud_model:
            try:
                
                condition_df = prediction_df
                # Dự đoán điều kiện (categorical)
                result_df = conditions_model.transform(condition_df)
                result_df = result_df.withColumnRenamed("prediction", "predicted_conditions_code")
                
                # Map numeric prediction back to string conditions
                result_df = result_df.withColumn("predicted_conditions", 
                                             map_conditions(col("predicted_conditions_code")))
                
                # Dự đoán nhiệt độ
                columns_to_keep = [c for c in result_df.columns if not (c.startswith("features") or c == "rawPrediction" or c == "probability")]
                clean_df = result_df.select(*columns_to_keep)
                temp_result_df = temp_model.transform(prediction_df)
                temp_result_df = temp_result_df.withColumnRenamed("prediction", "predicted_temp")
                temp_value = temp_result_df.select("predicted_temp").first()["predicted_temp"]
                result_df = clean_df.withColumn("predicted_temp", lit(temp_value))
                # Dự đoán mây
                cloud_result_df = cloud_model.transform(prediction_df)
                cloud_result_df = cloud_result_df.withColumnRenamed("prediction", "predicted_cloud")
                cloud_value = cloud_result_df.select("predicted_cloud").first()["predicted_cloud"]
                result_df = result_df.withColumn("predicted_cloud", lit(cloud_value))
                # Tính thời gian dự đoán (thời gian hiện tại + 3 giờ)
                result_df = result_df.withColumn("prediction_time", 
                                        F.expr("date_add(date, CASE WHEN hour + 3 >= 24 THEN 1 ELSE 0 END)"))
                result_df = result_df.withColumn("prediction_hour", 
                                        F.expr("(hour + 3) % 24"))
                
                result_df = result_df.withColumn("prediction_time_str", 
                                        F.concat(
                                            col("prediction_time").cast("string"), 
                                            lit(" "), 
                                            F.lpad(col("prediction_hour").cast("string"), 2, "0"),
                                            lit(":00:00")
                                        ))
                output_cols = [
                    "location", "date", "Time", 
                    "predicted_conditions", "predicted_temp", "predicted_cloud", "prediction_time_str"
                ]
                    
                output_df = result_df.select(output_cols)
                
                # Lưu kết quả dự đoán
                output_df.write \
                    .format("csv") \
                    .mode("append") \
                    .option("header", "true") \
                    .save("hdfs://namenode:9000/tmp/weather_data/predictions")
                
                # Hiển thị kết quả
                print(f"Predictions for location: {location_name}")
                output_df.show(truncate=False)
                
            except Exception as e:
                print(f"Lỗi khi dự đoán cho location {location_name}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("Models not available - skipping predictions")

# Tạo hàm để xử lý stream lỗi
def start_streaming():
    try:
        # Sử dụng foreachBatch để xử lý từng batch
        query = df_selected.writeStream \
            .foreachBatch(process_batch_with_prediction) \
            .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpoints/predictions") \
            .trigger(processingTime="5 seconds") \
            .start()

        # Giữ stream dữ liệu gốc cho lịch sử (tùy chọn - có thể bỏ nếu gặp lỗi)
        try:
            history_query = df_selected.writeStream \
                .format("csv") \
                .partitionBy("location") \
                .option("path", "hdfs://namenode:9000/tmp/weather_data/current") \
                .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpoints") \
                .option("header", "true") \
                .outputMode("append") \
                .trigger(processingTime="5 seconds") \
                .start()
                
            print("Cả hai stream đều đã khởi động thành công")
            
            query.awaitTermination()
            history_query.awaitTermination()
        except Exception as e:
            print(f"Lỗi khi khởi động history_query: {e}")
            # Nếu history_query gặp vấn đề, vẫn chạy query chính
            print("Tiếp tục với query chính")
            query.awaitTermination()
    except Exception as e:
        print(f"Lỗi khi khởi động stream: {e}")
        
# Chờ kết thúc
print("Starting streams. Waiting for termination...")
start_streaming()