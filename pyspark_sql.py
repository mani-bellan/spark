import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

fhvhvFile = spark.read.parquet("fhvhv_tripdata_2021-01.parquet")

fhvhvFile.createOrReplaceTempView("fhvhv")
df = spark.sql("SELECT hvfhs_license_num,pickup_datetime,dropoff_datetime FROM fhvhv WHERE hvfhs_license_num = 'HV0003' limit 10")
print(df.show())