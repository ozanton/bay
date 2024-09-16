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
                                'owner': 'airflow',
                                'start_date':datetime(2022, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "_4_friends_recommendation_mart",
                        default_args=default_args,
                        schedule_interval=None,
                        )


friends_recommendation_mart = SparkSubmitOperator(
                        task_id='friends_recommendation_mart_task',
                        dag=dag_spark,
                        application ='/lessons/scripts/mart_friends_recommendation.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['/user/vasiliikus/analytics/project_7/data/cities/geo.csv',
                                            '/user/vasiliikus/analytics/project_7/data/events',
                                            '/user/vasiliikus/analytics/project_7/mart'],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

friends_recommendation_mart