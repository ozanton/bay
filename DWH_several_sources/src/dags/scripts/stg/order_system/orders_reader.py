from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class OrdersReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_orders(self, load_threshold: datetime) -> List[Dict]:
        filter = {'update_ts': {'$gt': load_threshold}}

        sort = [('update_ts', 1)]

        docs = list(self.dbs.get_collection("orders").find(filter=filter, sort=sort))
        return docs
