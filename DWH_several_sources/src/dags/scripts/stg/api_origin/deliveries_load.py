from logging import Logger
from typing import List

from scripts.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
import datetime
import requests
import json

api_key = '______________________'
nickname = '_____________________'
cohort = '__'
sort_field = 'order_ts'
sort_direction = 'asc'
limit = 50
offset = 0
deliveries_base_url = "________________________________________________-"


class InputParams:
    headers = {
        "X-Nickname" : nickname,
        "X-Cohort" : cohort,
        "X-API-KEY" : api_key
        }
    params = {
        "sort_field" : sort_field, 
        "sort_direction" : sort_direction,
        "limit" : limit,
        "offset" : offset
        }

class ApiOriginRepository:
    def __init__(self, apiurl, ip: InputParams):
        self.iparam = ip
        self.base_url = apiurl
            

    def get_data(self):
        lst = []
        while True:
            r = requests.get(url = self.base_url, params = self.iparam.params, headers = self.iparam.headers)
            r_dict = json.loads(r.content)
            lst = r_dict + lst
            self.iparam.params['offset'] += 50
            if len(r_dict) == 0:
                self.iparam.params['offset'] = 0
                break
        new_list = [[el] for el in lst]
        for el in new_list:
            el.append(datetime.datetime.now().isoformat())
        return new_list


class ApiDestRepository:
    def insert_data(self, conn: Connection, row) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_deliveries (object_value, update_ts)
                    VALUES (%(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_value) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "object_value": '{}'.format(row[0]).replace("'", "\""),
                    "update_ts": row[1]
                },
            )


class DeliveriesLoader:
    WF_KEY = "api_deliveries_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_time"


    def __init__(self, url: deliveries_base_url, ip: InputParams, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ApiOriginRepository(url, ip)
        self.stg = ApiDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log
        


    def load_data(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: '2022-11-25%2000:00:00'})

            load_queue = self.origin.get_data()

            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for row in load_queue:
                self.stg.insert_data(conn, row)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[1] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")