import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'


args = {
    "owner": "etl_user",
    'email': ['email@email.ru'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
        'dds_update_dm_orders',
        schedule_interval='0 0 * * *',
        default_args=args,
        description='update dm_orders table with the courier_id, delivery_ts_id, sum and tip_sum data from stg.api_deliveries',
        catchup=True,
        tags=['update', 'dds'],
        start_date=datetime.today() - timedelta(days=7),
        max_active_runs = 1,
) as dag:


    update_dm_orders_table = PostgresOperator(
        task_id='update_dm_orders_courier_id',
        postgres_conn_id=postgres_conn_id,
        sql="update_dm_orders.sql")

    (
            update_dm_orders_table
    )