import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName('readParquet') \
    .getOrCreate()

#Read Green Taxi Data from the parquet file
df_green_taxi = spark.read.parquet('data/pq/green/*/*')

#rename the pickup and dropoff date times to have a generic name
df_green_taxi = df_green_taxi \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


#Read Yellow Taxi Data from the parquet file
df_yellow_taxi = spark.read.parquet('data/pq/yellow/*/*')

#rename the pickup and dropoff date times to have a generic name
df_yellow_taxi = df_yellow_taxi \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


#create a list of all common columns from these dataframes so that the respectvie data can be merged
common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]

#Add additional column called service type to the dataframes so identify if the data is a green taxi or a yellow taxi data
df_green_sel = df_green_taxi \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))


df_yellow_sel = df_yellow_taxi \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


#Union the data sets
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

#Register a temp table in spark to be used to write SQL queries

df_trips_data.registerTempTable('taxi_trips_data')

df_final = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    taxi_trips_data
GROUP BY
    1, 2, 3
""")

df_final.show()