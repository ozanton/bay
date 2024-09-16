from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str,str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class SalesObj(BaseModel):
    id: int
    event_ts: datetime
    event_value: str


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_sales(self, sales_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
                    select id, event_ts, event_value
                    from stg.bonussystem_events
                    WHERE event_type = 'bonus_transaction' and id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SalesDestRepository:
    
    def insert_sales(self, conn: Connection, sales: SalesObj) -> None:
        
        with conn.cursor() as cur:
            for pp in str2json(sales.event_value)['product_payments']:
                cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                        select dp.id, do2.id, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s
                        from dds.dm_orders do2 
                        join dds.dm_products dp on dp.product_id  = %(event_product_id)s
                        where do2.order_key = %(order_id)s
                        
                        
                        ON CONFLICT (id) DO UPDATE
                        SET
                            product_id = EXCLUDED.product_id,
                            order_id = EXCLUDED.order_id,
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant;
                    """,
                    {   
                        "count": pp['quantity'],
                        "price": pp['price'],
                        "order_id": str2json(sales.event_value)['order_id'],
                        "total_sum": pp['product_cost'],
                        "bonus_payment": pp['bonus_payment'],
                        "bonus_grant": pp['bonus_grant'],
                        "event_product_id": pp['product_id']
                    },
                )


class SalesLoader:
    WF_KEY = "sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = SalesOriginRepository(pg_origin)
        self.dds = SalesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_sales(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for sales in load_queue:
                self.dds.insert_sales(conn, sales)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
