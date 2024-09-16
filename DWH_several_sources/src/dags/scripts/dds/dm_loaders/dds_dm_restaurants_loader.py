from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class RestaurantsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_restaurants(self, restaurants_threshold: int, limit: int) -> List[RestaurantsObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantsObj)) as cur:
            cur.execute(
                """
                    select id, 
                           object_value::JSON->>'_id' as restaurant_id,
                           object_value::JSON->>'name' as restaurant_name,
                           update_ts as active_from,
                           '2099-12-31 00:00:00.000' :: timestamp as active_to 
                    from stg.ordersystem_restaurants
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": restaurants_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RestaurantsDestRepository:
    
    def insert_restaurants(self, conn: Connection, restaurants: RestaurantsObj) -> None:
        
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "id": restaurants.id,
                    "restaurant_id": restaurants.restaurant_id,
                    "restaurant_name": restaurants.restaurant_name,
                    "active_from": restaurants.active_from,
                    "active_to": restaurants.active_to
                },
            )


class RestaurantsLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 5

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = RestaurantsOriginRepository(pg_origin)
        self.dds = RestaurantsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):

        with self.pg_dest.connection() as conn:


            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return


            for restaurants in load_queue:
                self.dds.insert_restaurants(conn, restaurants)


            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
