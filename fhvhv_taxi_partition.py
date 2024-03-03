import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .parquet('fhvhv_tripdata_2021-01.parquet')

df = df.repartition(24)
df.write.parquet('fhvhv/2021/01/')
print(df.show(10))

