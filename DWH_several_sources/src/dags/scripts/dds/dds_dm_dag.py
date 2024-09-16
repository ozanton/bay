import logging

import pendulum
from airflow.decorators import dag, task
from scripts.dds.dm_loaders.dds_dm_users_loader import UsersLoader
from scripts.dds.dm_loaders.dds_dm_restaurants_loader import RestaurantsLoader
from scripts.dds.dm_loaders.dds_dm_timestamps_loader import TimestampsLoader
from scripts.dds.dm_loaders.dds_dm_products_loader import ProductsLoader
from scripts.dds.dm_loaders.dds_dm_orders_loader import OrdersLoader
from scripts.dds.dm_loaders.dds_fct_product_sales_loader import SalesLoader
from scripts.dds.dm_loaders.dds_dm_couriers_loader import CouriersLoader
from scripts.dds.dm_loaders.dds_dm_delivery_timestamps_loader import DeliveryTimestampsLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2022, 11, 25, tz="UTC"),  
    catchup=False,  
    tags=['sprint5', 'dds', 'stg'],  
    is_paused_upon_creation=True  
)
def stg_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = dwh_pg_connect
    
    @task(task_id="users_load")
    def load_users():
        rest_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  
    
    @task(task_id="restaurants_load")
    def load_restaurants():
        rest_loader = RestaurantsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants() 

    @task(task_id="timestamps_load")
    def load_timestamps():
        rest_loader = TimestampsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps() 

    @task(task_id="products_load")
    def load_products():
        rest_loader = ProductsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products() 

    @task(task_id="orders_load")
    def load_orders():
        rest_loader = OrdersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  

    @task(task_id="sales_load")
    def load_sales():
        rest_loader = SalesLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_sales() 
    
    @task(task_id="couriers_load")
    def load_couriers():
        rest_loader = CouriersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers() 

    @task(task_id="deliveries_timestamps_load")
    def load_delivery_timestamps():
        rest_loader = DeliveryTimestampsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_delivery_timestamps()



    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    sales_dict = load_sales()
    couriers_dict = load_couriers()
    delivery_timestamps_dict = load_delivery_timestamps()


    [users_dict, restaurants_dict, timestamps_dict, couriers_dict, delivery_timestamps_dict] >> products_dict >> orders_dict >> sales_dict

stg_dds_dm_dag = stg_dds_dag()



