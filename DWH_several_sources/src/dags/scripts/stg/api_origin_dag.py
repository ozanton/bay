import logging

import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from scripts.stg.api_origin.couriers_load import CouriersLoader, InputParams as CouriersInputParams, base_url
from scripts.stg.api_origin.restaurants_load import RestaurantsLoader, InputParams as RestInputParams, restaurants_base_url
from scripts.stg.api_origin.deliveries_load import DeliveriesLoader, InputParams as DelInputParams, deliveries_base_url


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval= '@daily', 
    start_date= datetime.today() - timedelta(days=7),
    catchup=True,
    max_active_runs = 1,
    tags=['api', 'stg', 'origin'],  
    is_paused_upon_creation=True  
)

def api_origin_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")


    @task(task_id="couriers_load")
    def load_couriers():
        couriers_loader = CouriersLoader(base_url, CouriersInputParams,dwh_pg_connect, log)
        couriers_loader.load_data()  


    @task(task_id="restaurants_load")
    def load_restaurants():
        
        restaurants_loader = RestaurantsLoader(restaurants_base_url, RestInputParams,dwh_pg_connect, log)
        restaurants_loader.load_data() 

    
    @task(task_id="deliveries_load")
    def load_deliveries():
        context = get_current_context()
        dt = str(context["logical_date"])
        start_date = dt.split('T')[0] + ' 00:00:00'
        end_date = dt.split('T')[0] + ' 23:59:59'

        DelInputParams.params['from'] = start_date
        DelInputParams.params['to'] = end_date

        deliveries_loader = DeliveriesLoader(deliveries_base_url, DelInputParams,dwh_pg_connect, log)
        deliveries_loader.load_data()  
        
        
   
    # Инициализируем объявленные таски.
    couriers_dict = load_couriers()
    restaurants_dict = load_restaurants()
    deliveries_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    [couriers_dict, restaurants_dict, deliveries_dict]


stg_couriers_api_origin_dag = api_origin_dag()
