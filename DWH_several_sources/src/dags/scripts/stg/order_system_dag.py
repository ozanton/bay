import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from scripts.stg.order_system.pg_saver import UsersPgSaver, OrdersPgSaver, RestaurantsPgSaver
from scripts.stg.order_system.users_loader import UsersLoader
from scripts.stg.order_system.users_reader import UsersReader
from scripts.stg.order_system.restaurant_loader import RestaurantLoader
from scripts.stg.order_system.restaurant_reader import RestaurantReader
from scripts.stg.order_system.orders_loader import OrdersLoader
from scripts.stg.order_system.orders_reader import OrdersReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2022, 11, 25, tz="UTC"),  
    catchup=False,  
    tags=['sprint5', 'stg', 'origin'],  
    is_paused_upon_creation=True  
)
def stg_order_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id="users_load")
    def load_users():
        pg_saver = UsersPgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = UsersReader(mongo_connect)

        loader = UsersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    @task(task_id="restaurants_load")
    def load_restaurants():
        pg_saver = RestaurantsPgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = RestaurantReader(mongo_connect)

        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    @task(task_id="orders_load")
    def load_orders():
        pg_saver = OrdersPgSaver()

        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        collection_reader = OrdersReader(mongo_connect)

        loader = OrdersLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    users_loader = load_users()
    restaurants_loader = load_restaurants()
    orders_loader = load_orders()

    [users_loader, restaurants_loader, orders_loader] 
    

order_system_stg_dag = stg_order_system()
