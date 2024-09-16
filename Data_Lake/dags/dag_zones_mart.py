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
                        dag_id = "zones_mart",
                        default_args=default_args,
                        schedule_interval=None,
                        )


zones_mart = SparkSubmitOperator(
                        task_id='zones_mart_task',
                        dag=dag_spark,
                        application ='/scripts/mart_zones.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['/user/analytics/project/data/cities/geo.csv',
                                            '/user/analytics/project/data/events',
                                            '/user/analytics/project/mart'],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

zones_mart