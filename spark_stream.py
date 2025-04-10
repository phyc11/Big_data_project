from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Khởi tạo Spark Session với thêm thư viện Elasticsearch connector
spark = SparkSession.builder \
    .appName("KafkaToElasticsearch") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "weather_forecast") \
    .option("startingOffsets", "earliest") \
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

# Parse JSON từ Kafka
df_selected = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), data_schema).alias("data")) \
    .select("data.*")

# # Thêm một trường id cho Elasticsearch (tùy chọn, nhưng hữu ích)
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

query = df_selected.writeStream \
    .format("csv") \
    .option("path", "/opt/bitnami/spark/output") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("header", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()

# Để theo dõi tiến trình, bạn có thể thêm một console output
console_query = df_selected.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# query.awaitTermination()
console_query.awaitTermination()