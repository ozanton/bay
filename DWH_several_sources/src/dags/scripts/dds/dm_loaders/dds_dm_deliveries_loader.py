from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DeliveriesObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime

class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_deliveries(self, deliveries_threshold: int, limit: int) -> List[DeliveriesObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveriesObj)) as cur:
            cur.execute(
                """
                    select id, object_value, update_ts 
                    from stg.api_deliveries
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": deliveries_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDestRepository:
    
    def insert_deliveries(self, conn: Connection, deliveries: DeliveriesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(id, delivery_id, address, rate, "sum", tip_sum)
                    VALUES (%(id)s, %(delivery_id)s, %(address)s, %(rate)s, %(sum)s, %(tip_sum)s )
                    ON CONFLICT (id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        address = EXCLUDED.address,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "id": deliveries.id,
                    "delivery_id": str2json(deliveries.object_value)['delivery_id'],
                    "address": str2json(deliveries.object_value)['address'],
                    "rate": str2json(deliveries.object_value)['rate'],
                    "sum": str2json(deliveries.object_value)['sum'],
                    "tip_sum": str2json(deliveries.object_value)['tip_sum']
                },
            )


class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DeliveriesOriginRepository(pg_origin)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for deliveries in load_queue:
                self.dds.insert_deliveries(conn, deliveries)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
