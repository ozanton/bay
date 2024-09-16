from logging import Logger
from typing import List

from scripts.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UsersObj(BaseModel):
    id: int
    object_value: str

class UserssOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
   
    def list_users(self, users_threshold: int, limit: int) -> List[UsersObj]:
        with self._db.client().cursor(row_factory=class_row(UsersObj)) as cur:
            cur.execute(
                """
                    select id, object_value, update_ts 
                    from stg.ordersystem_users
                    WHERE id > %(threshold)s 
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": users_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class UsersDestRepository:
    
    def insert_users(self, conn: Connection, users: UsersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(id, user_id, user_name, user_login)
                    VALUES (%(id)s, %(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "id": users.id,
                    "user_id": str2json(users.object_value)['_id'],
                    "user_name": str2json(users.object_value)['name'],
                    "user_login": str2json(users.object_value)['login']
                },
            )


class UsersLoader:
    WF_KEY = "users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = UserssOriginRepository(pg_origin)
        self.dds = UsersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_users(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})


            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for users in load_queue:
                self.dds.insert_users(conn, users)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
