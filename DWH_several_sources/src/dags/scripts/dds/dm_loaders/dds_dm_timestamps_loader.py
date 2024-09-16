from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class TimestampsObj(BaseModel):
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time

class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_timestamps(self, timestamps_threshold: datetime, limit: int) -> List[TimestampsObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampsObj)) as cur:
            cur.execute(
                """
                    select (object_value::JSON->>'date') :: timestamp as ts,
                           extract (year from (object_value::JSON->>'date') :: date) as year,
                           extract (month from (object_value::JSON->>'date') :: date) as month,
                           extract (day from (object_value::JSON->>'date') :: date) as day,
                           (object_value::JSON->>'date') :: date as date,
                           (object_value::JSON->>'date') :: time as time
                    from stg.ordersystem_orders
                    WHERE (object_value::JSON->>'date') :: timestamp > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampsDestRepository:
    
    def insert_timestamps(self, conn: Connection, timestamps: TimestampsObj) -> None:
        
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    -- ON CONFLICT (ts) DO UPDATE
                    -- SET
                    --     ts = EXCLUDED.ts,
                    --     year = EXCLUDED.year,
                    --     month = EXCLUDED.month,
                    --     day = EXCLUDED.day,
                    --     date = EXCLUDED.date,
                    --     time = EXCLUDED.time;
                """,
                {
                    "ts": timestamps.ts,
                    "year": timestamps.year,
                    "month": timestamps.month,
                    "day": timestamps.day,
                    "date": timestamps.date,
                    "time": timestamps.time
                },
            )


class TimestampsLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = TimestampsOriginRepository(pg_origin)
        self.dds = TimestampsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):

        with self.pg_dest.connection() as conn:


            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: '2022-01-01'})


            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return


            for timestamps in load_queue:
                self.dds.insert_timestamps(conn, timestamps)


            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
