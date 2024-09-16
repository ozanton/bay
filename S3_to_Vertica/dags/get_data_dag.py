from typing import List
 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.hooks.base import BaseHook
import pendulum
import boto3
import json

conn_s3 = BaseHook.get_connection('S3_CONN')


def fetch_s3_file(bucket: str, keys: List[str]):
    AWS_ACCESS_KEY_ID = json.loads(conn_s3.extra)['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = json.loads(conn_s3.extra)['AWS_SECRET_ACCESS_KEY']
 
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url= conn_s3.host, 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
 
    for key in keys:
        s3_client.download_file(
            Bucket='sprint6',
            Key=key,
            Filename=f'/data/{key}'
        )
 
bash_command_tmpl = 'head -10 {{ params.files }}'
 
@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def dag_get_data():
    bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']
    task1 = PythonOperator(
        task_id=f'fetch_files',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'data-bucket', 'keys': bucket_files},
    )
    
    get_files = task1

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': ' '.join([f'/data/{f}' for f in bucket_files])}
    )

    print_lines = print_10_lines_of_each

    get_files >> print_lines
 
_ = dag_get_data()