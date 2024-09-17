import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db



    def counters_truncate(self) -> None:
    
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                "truncate cdm.user_category_counters restart identity;"
                )
                
                cur.execute(
                "truncate cdm.user_product_counters restart identity;"
                )


    def user_category_counters_insert(self, user_id: str,
                                category_id: str,
                                category_name: str,
                                order_cnt: int
                                ) -> None:
    
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """ 
                    INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
                    VALUES(%(user_id)s, %(category_id)s,%(category_name)s, %(order_cnt)s);
                """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )


    def user_product_counters_insert(self, user_id: str,
                                product_id: str,
                                product_name: str,
                                order_cnt: int
                                ) -> None:
    
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """                     
                    INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
                    VALUES(%(user_id)s, %(product_id)s,%(product_name)s, %(order_cnt)s);
                """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name,
                        'order_cnt': order_cnt
                    }
                )