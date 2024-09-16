from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'etl_user',
                                'start_date':datetime(2022, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "partitions_overwrite",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application ='/scripts/data_partitions_overwrite.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2022-06-21",'/user/master/data/geo/events/','/user/analytics/project/data/events'],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

spark_submit_local