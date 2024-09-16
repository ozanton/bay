import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import sys

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
    df_messages = read_messages(base_input_path, sql).sample(0.1,123)
    df_subscriptions = read_subscriptions(base_input_path, sql).sample(0.1,123)
    df_geo = events_combined_by_cities(df_cities, df_messages)
    
    paired_messages = messages_from_to_from(df_geo)

    users = users_left_right(df_subscriptions)
    users = remove_duplicates(users)
    users = find_all_users_no_contact(users, paired_messages)
    users = filter_users_by_distance(users, df_geo)

    df_mart = mart(users)
    df_mart.write.mode("overwrite").parquet(f"{base_output_path}/friends_recommendation_mart")
       

def read_cities(path, spark):
    df = spark.read.option("header", True)\
              .option("delimiter", ";")\
              .csv(f'{path}')\
              .withColumn('lat_n', F.regexp_replace('lat', ',' , '.').cast('float'))\
              .withColumn('lng_n', F.regexp_replace('lng', ',' , '.').cast('float'))\
              .withColumn('lat_rad', F.radians('lat_n'))\
              .withColumn('lon_rad', F.radians('lng_n'))\
              .drop("lat","lng","lat_n","lng_n")\
              .persist()
    return df


def read_messages(base_path, spark):
    df = spark.read.parquet(f'{base_path}')\
              .where('lat is not null and lon is not null and event_type = "message"')\
              .withColumn('msg_lat_rad', F.radians('lat'))\
              .withColumn('msg_lon_rad', F.radians('lon'))\
              .drop('lat', 'lon')\
              .persist()
    return df

def read_subscriptions(base_path, spark):
    df = spark.read.parquet(f'{base_path}')\
              .where('event_type = "subscription"')\
              .select(F.col('event.subscription_channel'),
                      F.col('event.user'))\
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
            .drop('lon_rad', 'lat_rad')\
            .withColumn("row_number", F.row_number().over(window))\
            .filter(F.col('row_number')==1)\
            .select (F.col('event.message_from'),
                     F.col('event.message_to'),
                     F.col('event.message_id'),
                     F.col('event.message_ts'),
                     F.col('id').alias('zone_id'),
                     F.col('timezone'),
                     F.col('msg_lon_rad'),
                     F.col('msg_lat_rad'))\
            .persist()
    return df


def messages_from_to_from(df):
    df1 = df.where('message_from is not null and message_to is not null')\
           .select(F.col('message_from'),
                   F.col('message_to'))\
           .withColumn('pairs', F.concat(F.col('message_from'), F.lit('-'), F.col('message_to')))
    
    df2 = df.where('message_from is not null and message_to is not null')\
           .select(F.col('message_from'),
                   F.col('message_to'))\
           .withColumn('pairs', F.concat(F.col('message_to'), F.lit('-'), F.col('message_from')))
    
    united = df1.union(df2).distinct().drop('message_from', 'message_to')\
                .persist()
    return united


def users_left_right(df):
    u1 = df.alias('u1').withColumnRenamed('user', 'user_left').select(F.col('user_left'),
                                                                              F.col('subscription_channel'))
    
    u2 = df.alias('u2').withColumnRenamed('user', 'user_right').select(F.col('user_right'),
                                                                              F.col('subscription_channel').alias('sc_right'))
    
    df = u1.join(u2, [F.col('u1.subscription_channel') == F.col('sc_right'), F.col('user_left') != F.col('user_right')], 'inner')\
           .drop('sc_right')
    return df
    


def remove_duplicates(df):
    df = df\
           .withColumn("user_id", F.concat(F.col("user_left"), F.lit(","), F.col("user_right")))\
           .withColumn("user_arr", F.split(F.col("user_id"), ","))\
           .withColumn("user_left", F.array_min(F.col("user_arr")))\
           .withColumn("user_right", F.array_max(F.col("user_arr")))\
           .drop("user_id", "user_arr")\
           .dropDuplicates()\
           .persist()
    return df

def find_all_users_no_contact(df, paired_messages):
    df = df\
            .withColumn('pairs', F.concat(F.col('user_left'), F.lit('-'), F.col('user_right')))\
            .join(paired_messages, 'pairs', "leftanti")\
            .drop('pairs', 'subscription_channel')
    return df

def filter_users_by_distance(df, df_geo):
    window = Window().partitionBy('message_from').orderBy(F.col('message_ts').desc())
    ER = 6371
    df_geo = df_geo\
                   .withColumn('row_number', F.row_number().over(window))\
                   .where('row_number = 1')
    
    df = df.join(df_geo.select(F.col('message_from').alias('user_left'),
                               F.col('msg_lat_rad').alias('lat_left'),
                               F.col('msg_lon_rad').alias('lon_left'),
                               F.col('zone_id').alias('zone_id'),
                               F.col('timezone')), 'user_left', 'left')
    df = df.join(df_geo.select(F.col('message_from').alias('user_right'),
                               F.col('msg_lat_rad').alias('lat_right'),
                               F.col('msg_lon_rad').alias('lon_right')), 'user_right', 'left')
    
    df = df.withColumn('distance',
            F.lit(2)*F.lit(ER)* 
            F.asin(
            F.sqrt(F.pow(F.sin((F.col('lat_left')-F.col('lat_right'))/F.lit(2)), 2)+ 
                   F.cos(F.col('lat_right'))*F.cos(F.col('lat_left'))* 
                   F.pow(F.sin(((F.col('lon_left')-F.col('lon_right'))/F.lit(2))), 2)
                  )
                   )
                      )\
           .where('distance <= 1')\
           .drop('distance', 'zone_id_r','lat_left', 'lat_right', 'lon_left', 'lon_right')\
           .persist()
    
    return df


def mart(df):
    df = df\
           .withColumn('processed_dttm', F.current_date())\
           .withColumn('local_time', F.from_utc_timestamp(F.current_timestamp(),F.col('timezone')))\
           .select(F.col('user_left'),
                   F.col('user_right'),
                   F.col('processed_dttm'),
                   F.col('zone_id'),
                   F.col('local_time'))
    return df


if __name__ == "__main__":
        main()