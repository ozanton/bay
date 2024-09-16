import logging
from airflow import DAG
from datetime import datetime, timedelta
from lib import ConnectionBuilder
from airflow.providers.postgres.operators.postgres import PostgresOperator

dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")


args = {
    "owner": "etl_user",
    'email': ['email@email.ru'],
    'email_on_failure': False,
    'email_on_retry': False
}

business_dt = '{{ ds }}'

with DAG(
        'cdm_reports_loader',
        schedule_interval='0 0 * * *',
        default_args=args,
        description='load data from dds to cdm',
        catchup=True,
        tags=['cdm'],
        start_date=datetime.today() - timedelta(days=30),
        max_active_runs = 1,
) as dag:

        update_cdm_dm_settlement_report_table = PostgresOperator(
                                                task_id='update_cdm_dm_settlement_report',
                                                postgres_conn_id="PG_WAREHOUSE_CONNECTION",
                                                sql="cdm_dm_settlement_report.sql")
        
        update_cdm_dm_courier_ledger_table = PostgresOperator(
                                                task_id='update_cdm_dm_courier_ledger',
                                                postgres_conn_id="PG_WAREHOUSE_CONNECTION",
                                                sql="cdm_dm_courier_ledger.sql")
        (
            [update_cdm_dm_settlement_report_table, update_cdm_dm_courier_ledger_table]
        )