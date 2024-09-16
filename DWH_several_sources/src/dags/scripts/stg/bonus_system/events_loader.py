from logging import Logger
from typing import List

from scripts.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class EventsObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, events_threshold: int, limit: int) -> List[EventsObj]:
        with self._db.client().cursor(row_factory=class_row(EventsObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": events_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class EventsDestRepository:

    def insert_event(self, conn: Connection, events: EventsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": events.id,
                    "event_ts": events.event_ts,
                    "event_type": events.event_type,
                    "event_value": events.event_value
                },
            )


class EventsLoader:
    WF_KEY = "events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 250000  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_events(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for event in load_queue:
                self.stg.insert_event(conn, event)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
