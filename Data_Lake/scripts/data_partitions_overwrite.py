import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        conf = SparkConf().setAppName(f"EventsGeoPartitioningJob-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        events = sql.read.parquet(f"{base_input_path}/date={date}")
        

        events\
        .write\
        .mode("overwrite")\
        .partitionBy('event_type')\
        .format('parquet')\
        .save(f"{base_output_path}/date={date}")


if __name__ == "__main__":
        main()