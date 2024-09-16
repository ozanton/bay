from airflow import DAG
from airflow.contrib.operators.vertica_operator import VerticaOperator
import pendulum

args = {
    "owner": "etl_user",
    'email': ['email@email.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
        'migration_to_dds',
        schedule_interval=None,
        default_args=args,
        description='Upload the data from to dds data vault from stg',
        catchup=False,
        tags=['dds'],
        start_date=pendulum.parse('2022-07-13')
) as dag:

        upload_data_dds = VerticaOperator(
                                                task_id='upload_data_to_dds',
                                                vertica_conn_id="VERTICA_CONN",
                                                sql="insert_data_to_dds.sql")
        
        (
            [upload_data_dds]
        )