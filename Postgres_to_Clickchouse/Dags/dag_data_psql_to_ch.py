from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta, time


# extract data from PostgreSQL
def extract_data_from_postgres(**kwargs):
    execution_date = kwargs['execution_date']
    formatted_date = execution_date.strftime('%Y-%m-%d')
    pg_hook = PostgresHook(postgres_conn_id='ats_call_postgreSQL_con')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    try:
        query = f"""
        SELECT call_id, date, time, caller_number, operator, receiver_number,
               caller_name, call_duration, call_answered, call_result,
               ad_channel, outgoing_call, outgoing_call_duration
        FROM calls
        WHERE date = %s
        """
        print(f"Executing query: {query} with date {formatted_date}")
        pg_cursor.execute(query, (formatted_date,))
        rows = pg_cursor.fetchall()

        if rows:
            print(f"Query returned {len(rows)} rows.")
        else:
            print("Query returned no rows.")

        columns = [desc[0] for desc in pg_cursor.description]
        data = [dict(zip(columns, row)) for row in rows]

        # object time to string
        for row in data:
            if isinstance(row['time'], time):  # Проверяем, является ли это экземпляром datetime.time
                row['time'] = row['time'].strftime('%H:%M:%S')

        # log
        print("Extracted Data:", data)

    except Exception as e:
        print(f"Error during data extraction: {e}")
        data = []

    finally:
        pg_cursor.close()
        pg_conn.close()

    return data

# format data for ClickHouse
def format_data_for_clickhouse(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data_from_postgres')

    if not data:
        return "SELECT 1 WHERE 1 = 0"  # 0 rows

    batch_size = 1000  # parts
    formatted_queries = []

    try:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            formatted_values = []
            for row in batch:
                call_id = row['call_id'] if row['call_id'] is not None else 0

                date_value = f"'{row['date']}'" if row['date'] else 'NULL'
                time_value = f"'{row['time']}'" if row['time'] else 'NULL'

                call_duration = row['call_duration'] if row['call_duration'] is not None else 0
                outgoing_call_duration = row['outgoing_call_duration'] if row['outgoing_call_duration'] is not None else 0

                formatted_row = (
                    f"({call_id}, {date_value}, {time_value}, '{row['caller_number']}', '{row['operator']}', "
                    f"'{row['receiver_number']}', '{row['caller_name']}', {call_duration}, "
                    f"{int(row['call_answered']) if row['call_answered'] is not None else 0}, "
                    f"{int(row['call_result']) if row['call_result'] is not None else 0}, "
                    f"'{row['ad_channel']}', {int(row['outgoing_call']) if row['outgoing_call'] is not None else 0}, "
                    f"{outgoing_call_duration})"
                )
                formatted_values.append(formatted_row)

            formatted_query = """
            INSERT INTO external_calls (call_id, date, time, caller_number, operator, receiver_number, caller_name, call_duration, call_answered, call_result, ad_channel, outgoing_call, outgoing_call_duration)
            VALUES {}
            """.format(", ".join(formatted_values))

            formatted_queries.append(formatted_query)

        final_query = " ".join(formatted_queries)

        # log
        print("Formatted Queries:", final_query)

    except Exception as e:
        print(f"Error during data formatting: {e}")
        final_query = "SELECT 1 WHERE 1 = 0"

    return final_query

dag = DAG(
    dag_id='dag_postgres_to_clickhouse',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(seconds=60),
    },
    description='Transfer data from PostgreSQL to ClickHouse daily',
    schedule_interval='0 8 * * *',
    tags=["postgres_to_clickhouse"],
)

# extract from PostgreSQL
extract_task = PythonOperator(
    task_id='extract_data_from_postgres',
    python_callable=extract_data_from_postgres,
    provide_context=True,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

# format data
format_task = PythonOperator(
    task_id='format_data_for_clickhouse',
    python_callable=format_data_for_clickhouse,
    provide_context=True,
    dag=dag,
)

# insert to ClickHouse
load_task = ClickHouseOperator(
    task_id='load_data_to_clickhouse',
    clickhouse_conn_id='ats_call_clickhouse_con',
    sql="{{ task_instance.xcom_pull(task_ids='format_data_for_clickhouse') }}",
    dag=dag,
)

extract_task >> format_task >> load_task
