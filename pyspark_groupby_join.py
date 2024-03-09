import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

#Read Green Taxi Data from the parquet file
df_green_taxi = spark.read.parquet('data/pq/green/*/*')

#rename the pickup and dropoff date times to have a generic name
df_green_taxi = df_green_taxi \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


df_green_taxi.registerTempTable('green')
df_green_revenue = spark.sql("""
SELECT 
    date_trunc('hour', pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS green_amount,
    COUNT(1) AS green_records
FROM
    green
WHERE
    pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")

df_green_revenue.registerTempTable('green_revenue')

#Read Yellow Taxi Data from the parquet file
df_yellow_taxi = spark.read.parquet('data/pq/yellow/*/*')

#rename the pickup and dropoff date times to have a generic name
df_yellow_taxi = df_yellow_taxi \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

df_yellow_taxi.registerTempTable('yellow')
df_yellow_revenue = spark.sql("""
SELECT 
    date_trunc('hour', pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS yellow_amount,
    COUNT(1) AS yellow_records
FROM
    yellow
WHERE
    pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")

df_yellow_revenue.registerTempTable('yellow_revenue')

#Read the zones data and register a temptable out of it
df_zones = spark.read.parquet('zones/*')
df_zones.registerTempTable('zones_data')

#Generate data from all the above temp tables

df_final_revenue = spark.sql("""
SELECT 
    y.hour,y.zone,y.yellow_amount,y.yellow_records,z.Borough,z.Zone 
    from yellow_revenue y
    join zones_data z
    on y.zone = z.LocationID                             
""")

df_final_revenue.show()

