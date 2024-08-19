from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.check_table_sensor import CheckTableSensor
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    #"trigger_rule": "all_success",  # правило выполнения
}

dag = DAG('dag_dm_call_ch_hourly', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["data mart calls hourly"])

wait_for_dag_data_upcall_ch = ExternalTaskSensor(
    task_id='wait_for_dag_data_upcall_ch',
    external_dag_id='dag_data_upcall_ch',
    external_task_id=None,
    mode='reschedule',
    poke_interval=10,
    dag=dag,
)

clear_dm_hourly = ClickHouseOperator(
    task_id='clear_dm_hourly',
    clickhouse_conn_id='ats_call_clickhouse_con',
    sql="""TRUNCATE TABLE dm_calls_comparison_day; 
    TRUNCATE TABLE dm_calls_comparison_hourly_weekly;
    """,
    dag=dag)

task_check_clear_dm_hourly = CheckTableSensor(
    task_id=f'task_check_clear_dm_hourly',
    timeout=1000,
    mode='reschedule',
    poke_interval=10,
    conn='ats_call_clickhouse_con',
    table_name=['dm_calls_comparison_day', 'dm_calls_comparison_hourly_weekly'],
    dag=dag
)

task3 = ClickHouseOperator(
    task_id='up_dm_calls_comparison_day',
    clickhouse_conn_id='ats_call_clickhouse_con',
    sql="""INSERT into dm_calls_comparison_day 
    SELECT
        calls_today,
        calls_previous_day,
        CASE 
            WHEN calls_previous_day = 0 THEN NULL
            ELSE (calls_today - calls_previous_day) * 100.0 / calls_previous_day
        END AS percentage_change
    FROM (
        SELECT
            (SELECT COUNT(*) 
             FROM calls AS c
             INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
             INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
             WHERE ct.hour_24 = toHour(now()) 
               AND cd.full_date = today()) AS calls_today,
            (SELECT COUNT(*) 
             FROM calls AS c
             INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
             INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
             WHERE ct.hour_24 = toHour(now() - INTERVAL 1 DAY)
               AND cd.full_date = today() - INTERVAL 1 DAY) AS calls_previous_day);""",
    dag=dag)

task4 = ClickHouseOperator(
    task_id='up_dm_calls_comparison_hourly_weekly',
    clickhouse_conn_id='ats_call_clickhouse_con',
    sql=""" INSERT into dm_calls_comparison_hourly_weekly
    SELECT
        calls_today AS calls_current_hour_today,
        calls_same_hour_same_day_last_week AS calls_same_hour_same_day_last_week,
        CASE 
            WHEN calls_same_hour_same_day_last_week = 0 THEN NULL
            ELSE (calls_today - calls_same_hour_same_day_last_week) * 100.0 / calls_same_hour_same_day_last_week
        END AS percentage_change
    FROM (
        SELECT
            (SELECT COUNT(*) 
             FROM calls AS c
             INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
             INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
             WHERE ct.hour_24 = toHour(now()) 
               AND cd.full_date = today()) AS calls_today,
            (SELECT COUNT(*) 
             FROM calls AS c
             INNER JOIN cd_date AS cd ON c.date_id = cd.date_id
             INNER JOIN cd_time AS ct ON c.time_id = ct.time_id
             WHERE ct.hour_24 = toHour(now()) 
               AND cd.full_date = today() - INTERVAL 1 WEEK) AS calls_same_hour_same_day_last_week); """,
    dag=dag)


wait_for_dag_data_upcall_ch.set_downstream(clear_dm_hourly)
# hourly
clear_dm_hourly.set_downstream(task_check_clear_dm_hourly)
task_check_clear_dm_hourly.set_downstream([task3, task4])



