from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str,str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class ProductsObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_products(self, products_threshold: int, limit: int) -> List[ProductsObj]:
        with self._db.client().cursor(row_factory=class_row(ProductsObj)) as cur:
            cur.execute(
                """
                    select id, object_value, update_ts
                    from stg.ordersystem_restaurants
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": products_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductsDestRepository:
    
    def insert_products(self, conn: Connection, products: ProductsObj) -> None:
        
        with conn.cursor() as cur:
            for product in str2json(products.object_value)['menu']:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                        VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                        ON CONFLICT (id) DO UPDATE
                        SET
                            product_id = EXCLUDED.product_id,
                            product_name = EXCLUDED.product_name,
                            product_price = EXCLUDED.product_price,
                            active_from = EXCLUDED.active_from,
                            active_to = EXCLUDED.active_to,
                            restaurant_id = EXCLUDED.restaurant_id;
                    """,
                    {   
                        "product_id": product['_id'],
                        "product_name": product['name'],
                        "product_price": product['price'],
                        "active_from": products.update_ts,
                        "active_to": '2099-12-31 00:00:00.000',
                        "restaurant_id": products.id
                },
               )


class ProductsLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductsOriginRepository(pg_origin)
        self.dds = ProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):

        with self.pg_dest.connection() as conn:


            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for products in load_queue:
                self.dds.insert_products(conn, products)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
