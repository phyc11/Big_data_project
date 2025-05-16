from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,ArrayType
from pyspark.sql.functions import to_timestamp, expr,to_date
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
spark = SparkSession.builder \
    .appName("WeatherForscast") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.4") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# try :
#     conditions_model_path = "hdfs://namenode:9000/model/rf_model/conditions_model"
#     temp_model_path = "hdfs://namenode:9000/model/rf_model/temp_model"
#     cloud_model_path = "hdfs://namenode:9000/model/rf_model/cloud_model"
#     conditions_model = RandomForestClassificationModel.load(conditions_model_path)
#     temp_model_path = RandomForestRegressionModel.load(temp_model_path)
#     cloud_model_path = RandomForestRegressionModel.load(cloud_model_path)
#     print("model load ok ")
# except Exception as e:
#     print(f"Error loading models: {e}")
#     conditions_model = None
#     temp_model = None
#     cloud_model = None

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "weather_forecast") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Định nghĩa schema
data_schema = StructType([
    StructField("location", StringType(), True),
    StructField("date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("conditions", StringType(), True),
    StructField("temp", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("cloud", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("Gust", DoubleType(), True),
    StructField("actual_hour",StringType(),True)

])

# Lấy JSON từ Kafka
df_selected = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ArrayType(data_schema)).alias("data")) \
    .select(explode(col("data")).alias("weather")) \
    .select("weather.*")


def preprocess_data(batch_df, batch_id):
    if batch_df.isEmpty():
        return 
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    processed_df = batch_df \
        .withColumn("date", to_date(col("date"))) \
        .withColumn("actual_hour", to_timestamp(col("actual_hour"), "HH:mm:ss"))
    processed_df = processed_df.withColumn("hour", F.hour(col("actual_hour")))
query = df_selected.writeStream \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/tmp/weather_data/current") \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpoints") \
    .option("header", "true") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()
console_query = df_selected.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
console_query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, to_json, struct,explode
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType,ArrayType
# from pyspark.sql.functions import to_timestamp, expr,to_date
# from pyspark.ml.classification import RandomForestClassificationModel
# spark = SparkSession.builder \
#     .appName("WeatherForscast") \
#     .config("spark.jars.packages", 
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.4") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .config("spark.es.nodes", "elasticsearch") \
#     .config("spark.es.port", "9200") \
#     .config("spark.es.nodes.wan.only", "true") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")


# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "broker:29092") \
#     .option("subscribe", "weather_forecast") \
#     .option("startingOffsets", "latest") \
#     .option("failOnDataLoss", "false") \
#     .load()

# # Định nghĩa schema
# data_schema = StructType([
#     StructField("location", StringType(), True),
#     StructField("date", StringType(), True),
#     StructField("Time", StringType(), True),
#     StructField("conditions", StringType(), True),
#     StructField("temp", DoubleType(), True),
#     StructField("rain", DoubleType(), True),
#     StructField("cloud", DoubleType(), True),
#     StructField("pressure", DoubleType(), True),
#     StructField("humidity", DoubleType(), True),
#     StructField("windspeed", DoubleType(), True),
#     StructField("Gust", DoubleType(), True),
#     StructField("actual_hour",StringType(),True)

# ])

# # Lấy JSON từ Kafka
# df_selected = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), ArrayType(data_schema)).alias("data")) \
#     .select(explode(col("data")).alias("weather")) \
#     .select("weather.*")

# # model_path = "hdfs://namenode:9000/model/rf_model/weather_model"
# # rf_model = RandomForestClassificationModel.load(model_path)
# query = df_selected.writeStream \
#     .format("csv") \
#     .option("path", "hdfs://namenode:9000/tmp/weather_data/current") \
#     .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpoints") \
#     .option("header", "true") \
#     .outputMode("append") \
#     .trigger(processingTime="5 seconds") \
#     .option("maxRecordsPerFile", 10) \
#     .start()
#     # .trigger(processingTime="5 seconds") \
#     # .option("maxRecordsPerFile", 1) \
    


# console_query = df_selected.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()
# console_query.awaitTermination()
# query.awaitTermination()

# Ghi vào Elasticsearch

# query_es = df_selected.writeStream \
#     .outputMode("append") \
#     .format("es") \
#     .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpointes") \
#     .option("es.resource", "weather_forecast/_doc") \
#     .start()

# query_es = df_selected.writeStream \
#     .outputMode("append") \
#     .format("es") \
#     .option("checkpointLocation", "/tmp/checkpoint") \
#     .option("es.resource", "weather_forecast/_doc") \
#     .start()



# query_es.awaitTermination()

# query = df_selected.writeStream \
#     .format("csv") \
#     .option("path", "/opt/bitnami/spark/output") \
#     .option("checkpointLocation", "/tmp/checkpoint") \
#     .option("header", "true") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()

# console_query = df_selected.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# console_query.awaitTermination()