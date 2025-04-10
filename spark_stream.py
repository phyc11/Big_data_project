from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("WeatherForscast") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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
    StructField("country", StringType(), True),
    StructField("time", StringType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("wind_kph", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure_mb", DoubleType(), True),
    StructField("precip_mm", DoubleType(), True),
    StructField("cloud", DoubleType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("uv", DoubleType(), True),
    StructField("condition", StringType(), True)
])

# Lấy JSON từ Kafka
df_selected = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), data_schema).alias("data")) \
    .select("data.*")


query = df_selected.writeStream \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/tmp/weather_data") \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpoints") \
    .option("header", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()

# Thêm một trường id cho Elasticsearch (tùy chọn, nhưng hữu ích)
# df_with_id = df_selected.withColumn("id", 
#                                     col("location") + "_" + col("time").cast("string"))

# # Chuyển thành JSON format cho Elasticsearch
# df_es = df_with_id.select(to_json(struct("*")).alias("value"))

# # Ghi vào Elasticsearch
# query = df_es.writeStream \
#     .outputMode("append") \
#     .format("es") \
#     .option("checkpointLocation", "/tmp/checkpoint") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .option("es.resource", "weather_forecast") \
#     .option("es.nodes.wan.only", "true") \
#     .start()

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