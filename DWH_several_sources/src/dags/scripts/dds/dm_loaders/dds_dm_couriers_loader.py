from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class CouriersObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime

class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_couriers(self, couriers_threshold: int, limit: int) -> List[CouriersObj]:
        with self._db.client().cursor(row_factory=class_row(CouriersObj)) as cur:
            cur.execute(
                """
                    select id, object_value, update_ts 
                    from stg.api_couriers
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": couriers_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CouriersDestRepository:
    
    def insert_couriers(self, conn: Connection, couriers: CouriersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(id, courier_id, courier_name)
                    VALUES (%(id)s, %(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "id": couriers.id,
                    "courier_id": str2json(couriers.object_value)['_id'],
                    "courier_name": str2json(couriers.object_value)['name']
                },
            )


class CouriersLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = CouriersOriginRepository(pg_origin)
        self.dds = CouriersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for couriers in load_queue:
                self.dds.insert_couriers(conn, couriers)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
