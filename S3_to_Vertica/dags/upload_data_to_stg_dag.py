from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.contrib.hooks.vertica_hook import VerticaHook
import pendulum
 
conn = VerticaHook('VERTICA_CONN').get_cursor()

def upload_data(keys, cur):
    for key, val in keys.items():
        cur.execute(f'''COPY STAGING.{key} ({val})
                        FROM LOCAL '/data/{key}.csv'
                        DELIMITER ','
                        SKIP 1
                        REJECTED DATA AS TABLE STAGING.{key}_rej
                        ;''')
 
 
@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def dag_load_data():
    bucket_files = {'users': 'id, chat_name, registration_dt, country,age',
              'groups': 'id, admin_id, group_name, registration_dt, is_private',
              'group_log': 'group_id, user_id, user_id_from, event, event_dt',
              'dialogs': 'message_id,message_ts,message_from,message_to,message,message_group'
              }
    load_data_task = PythonOperator(
        task_id=f'load_files',
        python_callable=upload_data,
        op_kwargs={'keys': bucket_files,
                   'cur' : conn},
    )
    
    load_data_task
 
_ = dag_load_data()