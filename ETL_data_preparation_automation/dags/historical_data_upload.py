import time
import requests
import json
import pandas as pd
import numpy as np
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql'

nickname = 'name'
cohort = '7'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

##init test connection
def init_connection(ti):
    ###POSTGRESQL settings###
    #set postgresql connectionfrom basehook
    dwh_hook = PostgresHook(postgres_conn_id)
    conn = dwh_hook.get_conn()
    cur = conn.cursor()
    cur.close()
    conn.close()

def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')

def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')

def upload_from_s3_to_pg(ti,nickname,cohort):
    report_id = ti.xcom_pull(key='report_id')

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}'

    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort)
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", nickname)
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)

    #insert to database
    
    dwh_hook = PostgresHook(postgres_conn_id)
    conn = dwh_hook.get_conn()
    cur = conn.cursor()

    #get customer_research
    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv") )
    df_customer_research.reset_index(drop = True, inplace = True)
    print(df_customer_research.head(1))
    insert_cr = "insert into staging.customer_research (date_id,category_id,geo_id,sales_qty,sales_amt) VALUES {cr_val};"
    i = 0
    step = int(df_customer_research.shape[0] / 100)
    while i <= df_customer_research.shape[0]:
        print('df_customer_research',i, end='\r')

        cr_val =  str([tuple(x) for x in df_customer_research.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}',cr_val))
        conn.commit()
        
        i += step+1

    #get order log
    print(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv"))
    df_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv") )
    df_order_log.reset_index(drop = True, inplace = True)
    print(df_order_log.head(1))
    insert_uol = "insert into staging.user_order_log (uniq_id, date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    i = 0
    step = int(df_order_log.shape[0] / 100)
    while i <= df_order_log.shape[0]:
        print('df_order_log',i, end='\r')
        
        uol_val =  str([tuple(x) for x in df_order_log.drop(columns = ['id'] , axis = 1).loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}',uol_val))
        conn.commit()
               
        i += step+1

    #get activity log
    df_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv") )
    df_activity_log.reset_index(drop = True, inplace = True)
    print(df_activity_log.head(1))
    insert_ual = "insert into staging.user_activity_log (uniq_id, date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = int(df_activity_log.shape[0] / 100)
    while i <= df_activity_log.shape[0]:
        print('df_activity_log',i, end='\r')
                
        if df_activity_log.drop(columns = ['id'] , axis = 1).loc[i:i + step].shape[0] > 0:
            ual_val =  str([tuple(x) for x in df_activity_log.drop(columns = ['id'] , axis = 1).loc[i:i + step].to_numpy()])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}',ual_val))
            conn.commit()
        
        i += step+1

    #get price_log

    df_price_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "price_log.csv"), header=None)
    df_price_log.columns = ['category_name', 'price']
    df_price_log.reset_index(drop = True, inplace = True)
    insert_pl = "insert into staging.price_log (category_name, price) VALUES {pl_val};"
    print(df_price_log.head(1))
    i = 0
    step = int(df_price_log.shape[0] / 100)
    while i <= df_price_log.shape[0]:
        print('df_price_log',i, end='\r')
        if df_price_log.loc[i:i + step].shape[0] > 0:
            pl_val =  str([tuple(x) for x in df_price_log.loc[i:i + step].to_numpy()])[1:-1]
            cur.execute(insert_pl.replace('{pl_val}',pl_val))
            conn.commit()
     
        i += step+1

    cur.close()
    conn.close()

    return 200

with DAG(
    dag_id='001_upload_data_dag',
    schedule_interval='@once',
    start_date=datetime.today(),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    init_connection = PythonOperator(task_id='init_connection',
                                     python_callable=init_connection)
    generate_report = PythonOperator(task_id='generate_report',
                                 python_callable=generate_report)
    get_report = PythonOperator(task_id='get_report',
                                python_callable=get_report)
    create_staging_tables = PostgresOperator(task_id='create_staging_tables',
                                             postgres_conn_id=postgres_conn_id,
                                             sql="sql/staging.create_tables.sql")
    create_mart_tables = PostgresOperator(task_id='create_mart_tables',
                                             postgres_conn_id=postgres_conn_id,
                                             sql="sql/mart.create_tables.sql")
    t_upload_from_s3_to_pg = PythonOperator(task_id='upload_from_s3_to_pg',
	    									python_callable=upload_from_s3_to_pg,
		    								op_kwargs={'nickname':nickname, 'cohort':cohort})
    add_status_columnn = PostgresOperator(task_id='first_migration_status_column',
                                          postgres_conn_id=postgres_conn_id,
                                          sql="migrations/add_status_column.sql")

    del_from_d_tables = PostgresOperator(
        task_id='empty_d_tables',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.del_from_d_tables.sql")
    
    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_hist.sql")

    (      
            init_connection
            >> generate_report
            >> get_report
            >> create_staging_tables
            >> create_mart_tables 
            >> t_upload_from_s3_to_pg
            >> add_status_columnn 
            >> del_from_d_tables
            >> [update_d_city_table, update_d_customer_table, update_d_item_table]
            >> update_f_sales
           
    )