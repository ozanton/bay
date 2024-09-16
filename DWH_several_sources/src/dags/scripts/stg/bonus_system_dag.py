import logging

import pendulum
from airflow.decorators import dag, task
from scripts.stg.bonus_system.ranks_loader import RankLoader
from scripts.stg.bonus_system.users_loader import UsersLoader
from scripts.stg.bonus_system.events_loader import EventsLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), 
    catchup=False,  
    tags=['sprint5', 'stg', 'origin'],  
    is_paused_upon_creation=True 
)
def stg_bonus_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks() 

    @task(task_id="users_load")
    def load_users():
        rest_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users() 

    @task(task_id="events_load")
    def load_events():
        rest_loader = EventsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()  

    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    [ranks_dict, users_dict, events_dict]  


stg_bonus_system_dag = stg_bonus_system_dag()
