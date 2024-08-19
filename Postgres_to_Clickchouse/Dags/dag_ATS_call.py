from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import subprocess
import json

# Определение аргументов по умолчанию
default_args = {
    'owner': 'etl_user',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 3,
    "retry_delay": timedelta(seconds=10),
}

# Определение DAG
dag = DAG(
    'ats_data_load',
    default_args=default_args,
    description='Getting ATS data and load into BD',
    schedule_interval='0 7 * * *',
    catchup=True,
    tags=["data ats", "sixth"],
)

# выполнение скрипта и получения данных
def run_get_data_script(**kwargs):
    current_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    command = ['/usr/bin/python3', '/airflow/scripts/dag_ATS_call/data_ATS_call.py', current_date]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"Script output: {result.stdout}")
        print(f"Script error (if any): {result.stderr}")

        # Проверка на пустой вывод
        if not result.stdout.strip():
            print(f"No data generated for date: {current_date}.")
            return

        try:
            json_data = json.loads(result.stdout)
            kwargs['ti'].xcom_push(key='ats_data', value=json_data)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            raise

    except subprocess.CalledProcessError as e:
        print(f"Error executing script: {e}")
        raise

# загрузка данных в базу данных
def load_data_to_bd(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='get_data', key='ats_data')

    # Проверка на наличие данных
    if not json_data:
        print("No data found in XComs. Skipping data load.")
        return

    print(f"JSON data from XComs: {json_data}")
    print(f"Data to be loaded into raw_ats_data: {json.dumps(json_data, indent=2)}")

    # Извлечение параметров подключения из op_kwargs
    connection = kwargs.get('postgre_conn')
    if connection is None:
        raise ValueError("No PostgreSQL connection provided.")

    # Формирование строки подключения SQLAlchemy
    conn_str = f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"

    try:
        # Создание движка и подключение к базе данных
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            # Вставка данных
            query = text("INSERT INTO raw_ats_data (raw_data) VALUES (:raw_data)")
            conn.execute(query, {'raw_data': json.dumps(json_data)})

            # Проверка успешности загрузки данных
            result = conn.execute(text('SELECT COUNT(*) FROM raw_ats_data'))
            count_after_load = result.scalar()
            print(f"Number of records in raw_ats_data after load: {count_after_load}")

            if count_after_load < 1:
                raise ValueError("No records were successfully loaded into the raw_ats_data table.")

    except Exception as e:
        print(f"Error loading data to database: {e}")
        raise

# Определение задач
get_data = PythonOperator(
    task_id='get_data',
    python_callable=run_get_data_script,
    provide_context=True,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_bd,
    op_kwargs={'postgre_conn': BaseHook.get_connection("ats_call_postgreSQL_con")},
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
get_data >> load_data
