from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,ArrayType
from pyspark.sql.functions import to_timestamp, expr,to_date
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
query = df_selected.writeStream \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/tmp/weather_data") \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpoints") \
    .option("header", "true") \
    .partitionBy("location") \
    .outputMode("append") \
    .start()
query.awaitTermination()

# Ghi vào Elasticsearch

# query_es = df_selected.writeStream \
#     .outputMode("append") \
#     .format("es") \
#     .option("checkpointLocation", "hdfs://namenode:9000/tmp/checkpointes") \
#     .option("es.resource", "weather_forecast/_doc") \
#     .start()
=======
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