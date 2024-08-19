from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta



default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 11),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

dag = DAG('dag_data_upcall_psql', default_args=default_args, schedule_interval='0 7 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["up data calls postgres"])

wait_for_ats_data_load = ExternalTaskSensor(
    task_id='wait_for_ats_data_load',
    external_dag_id='ats_data_load',
    external_task_id=None,
    mode='reschedule',
    poke_interval=10,
    dag=dag,
)

up_data = PostgresOperator(
    task_id='up_data_postgres',
    postgres_conn_id="ats_call_postgreSQL_con",
    sql=f"""
    INSERT INTO calls (date, time, caller_number, operator, receiver_number, caller_name, call_duration,
        call_answered, call_result, ad_channel, outgoing_call, outgoing_call_duration)
    SELECT
        (json_data->>'datetime')::timestamp::date AS date,
        (json_data->>'datetime')::timestamp::time AS time,
        json_data->>'caller_number' AS caller_number,
        json_data->>'operator' AS operator,
        json_data->>'receiver_number' AS receiver_number,
        json_data->>'caller_name' AS caller_name,
        (json_data->>'call_duration')::integer AS call_duration,
        (CASE 
            WHEN json_data->>'call_answered' IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'on', 'off') 
            THEN (json_data->>'call_answered')::boolean
            ELSE NULL
        END) AS call_answered,
        (CASE 
            WHEN json_data->>'call_result' IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'on', 'off') 
            THEN (json_data->>'call_result')::boolean
            ELSE NULL
        END) AS call_result,
        json_data->>'ad_channel' AS ad_channel,
        (CASE 
            WHEN json_data->>'outgoing_call' IN ('true', 'false', 't', 'f', '1', '0', 'yes', 'no', 'on', 'off') 
            THEN (json_data->>'outgoing_call')::boolean
            ELSE NULL
        END) AS outgoing_call,
        (json_data->>'outgoing_call_duration')::integer AS outgoing_call_duration
    FROM
        raw_ats_data,
        jsonb_array_elements(raw_data::jsonb) AS json_data;
    WHERE
        (json_data->>'datetime')::date = '{{{{ execution_date }}}}'::date;
    """,
    dag=dag
)

change_status = PostgresOperator(
    task_id='change_status',
    postgres_conn_id="ats_call_postgreSQL_con",
    sql=f"""
    WITH expanded_data AS (
    SELECT
        c.call_id,
        r.id AS raw_id, 
        (json_data->>'datetime')::timestamp AS datetime
    FROM
        raw_ats_data r,
        jsonb_array_elements(r.raw_data::jsonb) AS json_data
    JOIN
        calls c
    ON
        c.date = (json_data->>'datetime')::date
        AND c.time = (json_data->>'datetime')::time
    WHERE
        (json_data->>'datetime')::date = '{{{{ execution_date }}}}'::date
)
INSERT INTO call_status (call_id, status, error_message)
SELECT
    call_id,
    'processed' AS status,
    NULL AS error_message
FROM
    expanded_data;
    """,
    dag=dag
)

up_status = PostgresOperator(
    task_id='up_status',
    postgres_conn_id="ats_call_postgreSQL_con",
    sql=f"""
    UPDATE raw_ats_data
    SET processing_status = 'processed'
    WHERE processing_status = 'unprocessed'
    AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements(raw_data::jsonb) AS json_data
        WHERE (json_data->>'datetime')::date = '{{{{ execution_date }}}}'::date
    );""",
    dag=dag
)

wait_for_ats_data_load >> up_data >> change_status >> up_status
