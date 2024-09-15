from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, to_json, col, lit, struct, from_unixtime, unix_timestamp

from datetime import datetime

TOPIC_IN = 'project'
TOPIC_OUT = 'project.out'

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0"
        ]
        )

def spark_init(test_name):
    spark = SparkSession.builder\
    .master("local")\
    .appName(test_name)\
    .config("spark.jars.packages", spark_jars_packages)\
    .getOrCreate()
    return spark


def read_clients(spark: SparkSession) -> DataFrame:
    tableDF = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'user') \
                    .option('password', 'password') \
                    .load()
    return tableDF

def read_actions(spark: SparkSession):
    df = (spark.readStream
                .format('kafka')
       .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
       .option('kafka.security.protocol', 'SASL_SSL')
       .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
       .option('kafka.sasl.jaas.config','org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"password\";')
       .option('subscribe',TOPIC_IN)
       .load())
    return df

def transform(df: DataFrame) -> DataFrame:
    schema = StructType([
            StructField("restaurant_id", StringType()),
            StructField("adv_campaign_id", StringType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", IntegerType()),
            StructField("adv_campaign_datetime_end", IntegerType()),
            StructField("datetime_created",  IntegerType()),
        ])
 
    return (df
            .withColumn('value', col('value').cast(StringType()))
            .withColumn('event', from_json(col('value'), schema))
            .selectExpr('event.*')
            .where(f'adv_campaign_datetime_start <= {current_timestamp_utc} and adv_campaign_datetime_end >= {current_timestamp_utc}')
            .withColumn('datetime_created_tmp',
                        from_unixtime(col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            .dropDuplicates(['restaurant_id', 'adv_campaign_id'])
            .withWatermark('datetime_created_tmp', '10 minutes')
            .drop('datetime_created_tmp')
            )


def joinDF(df_clients, df_actions):
    return (df_actions
                .join(df_clients, 'restaurant_id')
                .drop('id')
                .withColumn('trigger_datetime_created', unix_timestamp())
           )


def foreach_batch_function(df, epoch_id):
    df.persist()

    df.withColumn('feedback', lit(None).cast(StringType())) \
        .write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "subscribers_feedback") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .mode("append") \
        .save()

    kafka_df = df.select(to_json( \
            struct("restaurant_id", \
                   "adv_campaign_id", \
                   "adv_campaign_content", \
                   "adv_campaign_owner", \
                   "adv_campaign_owner_contact", \
                   "adv_campaign_datetime_start", \
                   "adv_campaign_datetime_end", \
                   "client_id", \
                   "datetime_created", \
                   "trigger_datetime_created")) \
            .alias("value"))

    (kafka_df.write
        .format("kafka") 
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.sasl.jaas.config','org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";')
        .option('topic',TOPIC_OUT)
        .save()
    )
 
    df.unpersist()

if __name__ == "__main__":
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
    spark = spark_init('project read actions stream')
    clients = read_clients(spark)
    source_df = read_actions(spark)
    output_df = transform(source_df)
    result_df = joinDF(clients, output_df)
    
    query = (result_df.writeStream
    .foreachBatch(foreach_batch_function)
    .start()
    .awaitTermination()
    )