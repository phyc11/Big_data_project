from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_date, concat_ws,date_format, to_timestamp,concat
from pyspark.sql.functions import regexp_replace, split, col, trim,when,lag
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
spark = SparkSession.builder \
    .appName("Read HDFS Weather Data") \
    .master("local") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()


def merge_file(list_locations):
    df_all = None  
    
    for location in list_locations:
        try:
            
            df = spark.read.option("multiLine", True) \
                .option("header", True) \
                .option("inferSchema", False) \
                .option("encoding", "utf-8") \
                .csv(f"hdfs://namenode:9000/tmp/weather_data/history/{location}.csv")
            df = df.withColumn("location", lit(location))
            df_all = df if df_all is None else df_all.unionByName(df)
        
        except AnalysisException as e:
            print(f"File không tồn tại hoặc không thể đọc: {location}. Lỗi: {e}")
        except Exception as e:
            print(f"Đã xảy ra lỗi với file {location}: {e}")
    
    return df_all
def data_process(df):
    df_clean=df.withColumn("Date",to_date(col("Date"),"yyyy-MM-dd")) \
               .withColumn("Time",date_format(col("Time"),"HH:mm")) \
               .withColumn("Temp(°c)",regexp_replace("Temp","°c","").cast("double")) \
               .withColumn("Rain(mm)", regexp_replace("Rain", "mm", "").cast("double")) \
               .withColumn("Cloud", (regexp_replace("Cloud", "%", "").cast("double") )) \
               .withColumn("Pressure(mb)", regexp_replace("Pressure", "mb", "").cast("double")) \
               .withColumn("Wind(km/h)", regexp_replace("Wind", "km/h", "").cast("double")) \
               .withColumn("Gust(km/h)", regexp_replace("Gust", "km/h", "").cast("double")) 
    df_clean = df_clean.withColumn("Timestamp", 
                                  to_timestamp(concat(col("Date").cast("string"), 
                                                     lit(" "), 
                                                     col("Time")), 
                                               "yyyy-MM-dd HH:mm"))
    df_clean = df_clean.drop("Time", "Date", "Temp", "Rain", "Cloud", "Pressure", "Wind", "Gust")
    return df_clean
northern_provinces = [
    'bac-can', 'bac-giang', 'bac-ninh', 'cao-bang', 
    'dien-bien', 'ha-giang', 'ha-noi', 'hai-duong', 
    'hai-phong', 'hanoi', 'hoa-binh', 'hong-gai', 
    'lang-son', 'lao-cai', 'nam-dinh', 'ninh-binh', 
    'phu-ly', 'son-la', 'son-tay', 'thai-binh', 
    'thai-nguyen', 'tuyen-quang', 'uong-bi', 'viet-tri', 
    'vinh-yen', 'cam-pha'
]

central_provinces = [
    'da-lat', 'dong-hoi', 'ha-tinh', 'hoi-an', 
    'hue', 'kon-tum', 'nha-trang', 'phan-rang', 
    'phan-thiet', 'play-cu', 'quang-ngai', 'qui-nhon', 
    'tam-ky', 'thanh-hoa', 'tuy-hoa', 'vinh', 
    'buon-me-thuot', 'cam-ranh'
]

southern_provinces = [
    'bac-lieu', 'ben-tre', 'bien-hoa', 'ca-mau', 
    'can-tho', 'chau-doc', 'dong-xoai', 'ho-chi-minh-city', 
    'long-xuyen', 'my-tho', 'rach-gia', 'soc-trang', 
    'tan-an', 'tay-ninh', 'tra-vinh', 'vinh-long', 
    'vung-tau'
]
df_north= merge_file(northern_provinces)
df_north=data_process(df_north)
df_central= merge_file(central_provinces)
df_central=data_process(df_central)
df_south= merge_file(southern_provinces)
df_south=data_process(df_south)
df_central.coalesce(1).write.format("csv").mode("overwrite").save("hdfs://namenode:9000/tmp/weather_data/central")
df_north.coalesce(1).write.format("csv").mode("overwrite").save("hdfs://namenode:9000/tmp/weather_data/north")
df_south.coalesce(1).write.format("csv").mode("overwrite").save("hdfs://namenode:9000/tmp/weather_data/south")