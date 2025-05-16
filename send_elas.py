from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,ArrayType, TimestampType
# Khởi tạo SparkSession
spark=SparkSession.builder.appName("music").master("spark://spark-master:7077").config("spark.jars.packages","org.elasticsearch:elasticsearch-spark-30_2.12:7.17.4") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.driver.extraClassPath","elasticsearch-hadoop-7.17.4.jar")\
                .config("spark.es.nodes","elasticsearch") \
                .config("spark.es.port","9200") \
                .config("spark.es.nodes.wan.only","true") \
                .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
elasticsearch_conf = {
            'es.nodes': "elasticsearch",
            'es.port': "9200",
            "es.nodes.wan.only": "true"
        }
regions = ["north", "central", "south"]
df = []
data_schema = StructType([
    StructField("Weather", StringType(), True),
    StructField("location", StringType(), True),
    StructField("Temp(°C)", DoubleType(), True),
    StructField("Rain(mm)", DoubleType(), True),
    StructField("Pressure(mb)", DoubleType(), True),
    StructField("Wind(km/h)", DoubleType(), True),
    StructField("Gust(km/h)", DoubleType(), True),
    StructField("Timestamp", StringType(), True)  
])

for region in regions:
    dataframe = spark.read.option("multiLine", True) \
                .option("header", True) \
                .option("encoding", "utf-8") \
                .schema(data_schema) \
                .csv(f"hdfs://namenode:9000/tmp/weather_data/{region}/*.csv")
    df.append(dataframe)
df_indexes=["north_weather","central_weather","south_weather"]
for dataframe, index in zip(df,df_indexes):
    print("write index : ", index)
    dataframe.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", f"{index}") \
        .options(**elasticsearch_conf) \
        .mode("overwrite") \
        .save()

spark.stop()