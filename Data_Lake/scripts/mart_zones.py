import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import sys

from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as F 
from pyspark.sql.window import Window
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        cities_path = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        conf = SparkConf().setAppName(f"UserMartJob")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        df_cities = read_cities(cities_path, sql)
        df_events = read_events(base_input_path, sql).sample(fraction= 0.05, seed=42)
        df_events_combined = events_combined_by_cities(df_events,df_cities)
        
        df_zones_mart = zones_mart(df_events_combined)
        df_zones_mart.write.mode("overwrite").parquet(f"{base_output_path}/zones_mart")
       


def read_cities(path, spark):
    df = spark.read.option("header", True)\
              .option("delimiter", ";")\
              .csv(f'{path}')\
              .withColumn('lat_n', F.regexp_replace('lat', ',' , '.').cast('float'))\
              .withColumn('lng_n', F.regexp_replace('lng', ',' , '.').cast('float'))\
              .withColumn('lat_rad', F.radians('lat_n'))\
              .withColumn('lon_rad', F.radians('lng_n'))\
              .drop("lat","lng","lat_n","lng_n")
    return df


def read_events(base_path, spark):
    df = spark.read.parquet(f'{base_path}')\
              .where('lat is not null and lon is not null')\
              .withColumn('msg_lat_rad', F.radians('lat'))\
              .withColumn('msg_lon_rad', F.radians('lon'))\
              .drop('lat', 'lon')\
              .persist()
    return df


def events_combined_by_cities(df_events, df_cities):
    window = Window().partitionBy('event.message_id').orderBy(F.col('distance').asc())
    ER = 6371
    
    df = df_events.crossJoin(df_cities)\
           .withColumn('distance',
            F.lit(2)*F.lit(ER)* 
            F.asin(
            F.sqrt(F.pow(F.sin((F.col('msg_lat_rad')-F.col('lat_rad'))/F.lit(2)), 2)+ 
                   F.cos(F.col('lat_rad'))*F.cos(F.col('msg_lat_rad'))* 
                   F.pow(F.sin(((F.col('msg_lon_rad')-F.col('lon_rad'))/F.lit(2))), 2)
                  )
                   )
                      )\
            .drop('msg_lat_rad', 'msg_lon_rad', 'lon_rad', 'lat_rad')\
            .withColumn("row_number", F.row_number().over(window))\
            .filter(F.col('row_number')==1)\
            .select (F.col('event.message_from').alias('user_id'),
                     F.col('event_type'),
                     F.col('id').alias('zone_id'),
                     F.col('date'))\
            .persist()
    return df


def zones_mart(df_geo):
    window = Window().partitionBy('user_id').orderBy(F.col('date').asc())
    window_month = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "month")])
    window_week = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "week")])

    df_registrations = df_geo\
                             .withColumn("row_number", F.row_number().over(window))\
                             .filter(F.col('row_number')==1)\
                             .withColumn("month",F.trunc(F.col("date"), "month"))\
                             .withColumn("week",F.trunc(F.col("date"), "week"))\
                             .withColumn("week_user", F.count('user_id').over(window_week))\
                             .withColumn("month_user", F.count('user_id').over(window_month))\
                             .select(F.col('month'),
                                     F.col('week'),
                                     F.col('week_user'),
                                     F.col('month_user'))\
                             .distinct()

    df = df_geo\
          .withColumn("month",F.trunc(F.col("date"), "month"))\
          .withColumn("week",F.trunc(F.col("date"), "week"))\
          .withColumn("week_message",F.sum(F.when(df_geo.event_type == "message",1)
                                            .otherwise(0)).over(window_week))\
          .withColumn("week_reaction",F.sum(F.when(df_geo.event_type == "reaction",1)
                                            .otherwise(0)).over(window_week))\
          .withColumn("week_subscription",F.sum(F.when(df_geo.event_type == "subscription",1)
                                                 .otherwise(0)).over(window_week))\
          .withColumn("month_message",F.sum(F.when(df_geo.event_type == "message",1)
                                             .otherwise(0)).over(window_month))\
          .withColumn("month_reaction",F.sum(F.when(df_geo.event_type == "reaction",1)
                                              .otherwise(0)).over(window_month))\
          .withColumn("month_subscription",F.sum(F.when(df_geo.event_type == "subscription",1)
                                                  .otherwise(0)).over(window_month))\
          .join(df_registrations, ["month", "week"], "fullouter")\
          .select(F.col('month'),
                  F.col('week'), 
                  F.col('zone_id'), 
                  F.col('week_message'), 
                  F.col('week_reaction'), 
                  F.col('week_subscription'), 
                  F.col('week_user'), 
                  F.col('month_message'), 
                  F.col('month_reaction'), 
                  F.col('month_subscription'),
                  F.col('month_user'))\
          .distinct()
    return df


if __name__ == "__main__":
        main()