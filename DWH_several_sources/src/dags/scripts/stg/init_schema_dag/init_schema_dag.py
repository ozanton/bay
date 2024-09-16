import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from scripts.stg.init_schema_dag.schema_init import SchemaDdl
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  
    catchup=False,  
    tags=['sprint5', 'stg', 'dds', 'cdm', 'schema', 'ddl'],  
    is_paused_upon_creation=True  
)
def init_schema_dag():
    
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    ddl_path = Variable.get("SCRIPTS_STG_DDL_FILES_PATH")

    @task(task_id="schema_init")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    init_schema = schema_init()

    init_schema  # type: ignore

stg_init_schema_dag = init_schema_dag()