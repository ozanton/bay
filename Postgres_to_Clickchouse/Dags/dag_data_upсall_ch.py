from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

dag = DAG('dag_data_upcall_ch', default_args=default_args, schedule_interval='0 8 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["data up calls clickhouse"])

wait_for_dag_data_psql_to_ch = ExternalTaskSensor(
    task_id='wait_for_dag_data_psql_to_ch',
    external_dag_id='dag_data_psql_to_ch',
    external_task_id=None,
    mode='reschedule',
    poke_interval=10,
    dag=dag,
)

up_calls_ch = ClickHouseOperator(
    task_id='up_calls_ch',
    clickhouse_conn_id='ats_call_clickhouse_con',
    sql="""INSERT INTO calls(
    call_id, 
    date_id, 
    time_id, 
    caller_number, 
    operator_id, 
    receiver_number, 
    caller_name_id,
    call_duration, 
    call_answered, 
    call_result, 
    ad_channel_id, 
    outgoing_call, 
    outgoing_call_duration
)
SELECT 
    ec.call_id, 
    cd.date_id, 
    ct.time_id, 
    ec.caller_number, 
    co.operator_id, 
    ec.receiver_number, 
    cn.caller_name_id, 
    ec.call_duration, 
    ec.call_answered, 
    ec.call_result, 
    ac.ad_channel_id, 
    ec.outgoing_call, 
    ec.outgoing_call_duration
FROM external_calls AS ec
INNER JOIN cd_date AS cd 
    ON ec.date = cd.full_date
INNER JOIN cd_time AS ct 
    ON substring(ec.time, 1, 8) = ct.full_time_24 
LEFT JOIN cd_adchannels AS ac 
    ON ec.ad_channel = ac.ad_channel_name
LEFT JOIN cd_operators AS co 
    ON ec.operator = co.operator_name
LEFT JOIN cd_callernames AS cn 
    ON ec.caller_name = cn.caller_name
WHERE ec.date = '{{ execution_date.strftime('%Y-%m-%d') }}'; """,
    dag=dag)

# daily
wait_for_dag_data_psql_to_ch.set_downstream(up_calls_ch)


