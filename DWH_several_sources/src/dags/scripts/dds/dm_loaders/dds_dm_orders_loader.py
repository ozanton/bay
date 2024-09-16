from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str,str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class OrdersObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_orders(self, orders_threshold: int, limit: int) -> List[OrdersObj]:
        with self._db.client().cursor(row_factory=class_row(OrdersObj)) as cur:
            cur.execute(
                """
                    select id, object_value, update_ts
                    from stg.ordersystem_orders
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": orders_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrdersDestRepository:
    
    def insert_orders(self, conn: Connection, orders: OrdersObj) -> None:
        
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    SELECT du.id, dr.id, dt.id, %(order_key)s, %(order_status)s 
                    FROM dds.dm_restaurants as dr
                    inner join dds.dm_users as du on du.user_id = %(user_id)s
                    inner join dds.dm_timestamps as dt on dt.ts = %(ts)s
                    WHERE dr.restaurant_id = %(restaurant_id)s
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status;
                """,
                {   
                    "user_id": str2json(orders.object_value)['user']['id'],
                    "restaurant_id": str2json(orders.object_value)['restaurant']['id'],
                    "ts": str2json(orders.object_value)['date'],
                    "order_key": str2json(orders.object_value)['_id'],
                    "order_status": str2json(orders.object_value)['final_status']
                },
            )


class OrdersLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = OrdersOriginRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for orders in load_queue:
                self.dds.insert_orders(conn, orders)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
