from airflow import DAG
from airflow.contrib.operators.vertica_operator import VerticaOperator
import pendulum

args = {
    "owner": "elt_user",
    'email': ['email@email.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
        'init_schema_stg_dds',
        schedule_interval=None,
        default_args=args,
        description='Create stg and dds schemas',
        catchup=False,
        tags=['init_schema'],
        start_date=pendulum.parse('2022-07-13'),
        template_searchpath = ['/sql']
) as dag:

        create_stg_schema_tables = VerticaOperator(
                                                task_id='create_stg_schema_tables',
                                                vertica_conn_id="VERTICA_CONN",
                                                sql="create_stg.sql")
        
        create_dds_schema_tables = VerticaOperator(
                                                task_id='create_dds_schema_tables',
                                                vertica_conn_id="VERTICA_CONN",
                                                sql="create_dds.sql")
        (
            [create_stg_schema_tables, create_dds_schema_tables]
        )