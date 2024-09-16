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
        df_messages = read_messages(base_input_path, sql)
        df_geo = distance_calc(df_cities, df_messages)

        df_users_mart = users_mart(df_geo)

        df_users_mart.write.mode("overwrite").parquet(f"{base_output_path}/users_mart")


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


def read_messages(base_path,spark):
    df = spark.read.parquet(f'{base_path}')\
              .where('event_type = "message" and lat is not null and lon is not null and event.message_ts is not null')\
              .select(F.col('event.message_id'),
                      F.col('event.message_from'),
                      F.col('event.message_to'),
                      F.col('event.message_ts'),
                      F.col('lat'),
                      F.col('lon'))\
              .withColumn('msg_lat_rad', F.radians('lat'))\
              .withColumn('msg_lon_rad', F.radians('lon'))\
              .drop('lat', 'lon')
    return df


def distance_calc(df_cities, df_messages):
    ER = 6371
    window = Window().partitionBy('message_id').orderBy(F.col('distance').asc())
    df = df_messages.crossJoin(df_cities)\
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
            .drop('row_number')\
            .persist()
    return df


def act_city(df_geo):
    window = Window().partitionBy('message_from').orderBy(F.col('message_ts').desc())
    df_geo = df_geo.withColumn('row_number', F.row_number().over(window))\
           .filter(F.col('row_number')==1)\
           .select(F.col('message_from').alias('user_id'),
                   F.col('city').alias('act_city'),
                   F.col('message_ts'),
                   F.col('timezone'))
    return df_geo


def home_city(df_geo):
    window = Window().partitionBy('user_id').orderBy(F.col('message_date').asc())
    window_grp = Window().partitionBy('user_id').orderBy(F.col('dates_seq_cnt').desc())
    df_geo = df_geo\
           .select(F.col('message_from').alias('user_id'),
                   F.col('message_ts'),
                   F.col('city'))\
           .withColumn('message_date', df_geo['message_ts'].cast('date'))\
           .withColumn('dense_rank', F.dense_rank().over(window))\
           .withColumn('grp', F.datediff(F.col('message_date'),F.to_date(F.col('dense_rank').cast("string"), 'd')))\
           .groupBy('user_id', 'grp', 'city').agg(F.countDistinct(F.col('dense_rank')).alias('dates_seq_cnt'))\
           .withColumn('tmp', F.row_number().over(window_grp))\
           .where('tmp = 1 and dates_seq_cnt >= 27')\
           .drop('dates_seq_cnt', 'tmp', 'grp')\
           .withColumnRenamed('city', 'home_city')
    return df_geo


def remove_trailing_elements(lst):
    for i in range(len(lst)-1, 0, -1):
        if lst[i] == lst[i-1]:
            lst.pop(i)
    return tuple(lst)


def travel_geo(df_geo):
    remove_trailing_elements_udf = F.udf(remove_trailing_elements, ArrayType(StringType()))

        
    windowSpec = Window.partitionBy('message_from').orderBy('message_ts')
    df_geo = df_geo.withColumn('visited_cities',F.collect_list('city').over(windowSpec))\
           .withColumn("travel_array", remove_trailing_elements_udf(F.col("visited_cities")))\
           .withColumn('travel_count', F.size(F.col('travel_array')))\
           .groupBy('message_from').agg(F.last('travel_count').alias('travel_count'),
                                        F.last('travel_array').alias('travel_array'))\
           .withColumnRenamed('message_from', 'user_id')
    return df_geo


def local_time(df):
    df = df.select(F.col('user_id'),
                   F.col('message_ts'),
                   F.col('timezone'))\
           .withColumn('local_time',F.from_utc_timestamp(F.col("message_ts"),F.col('timezone')))\
           .drop('message_ts', 'timezone')
    return df


def users_mart(df_geo):
    df_act_city = act_city(df_geo)
    df_act_city.persist()
    df_home_city = home_city(df_geo)
    df_travel_geo = travel_geo(df_geo)
    
    df_tmp = df_act_city.drop('timezone').join(df_home_city, "user_id", "left")\
                    .drop('message_ts')\
                    .join(df_travel_geo, "user_id", "left")
    
    df = df_tmp.join(local_time(df_act_city), "user_id", "left")
    return df


if __name__ == "__main__":
        main()