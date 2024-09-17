from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        self._cdm_repository.counters_truncate()
        
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break


            cdm_cat = msg['users_categories']
            cdm_prod = msg['users_products']
            for cat in cdm_cat:
                self._cdm_repository.user_category_counters_insert(
                    cat['user_id'],
                    cat['category_id'],
                    str(cat['category_name']),
                    cat['order_cnt']
                )
            self._logger.info(f"{datetime.utcnow()}: category cnt has been recorder into CDM")

            
            for pr in cdm_prod: 
                self._cdm_repository.user_product_counters_insert(
                    pr['user_id'],
                    pr['product_id'],
                    str(pr['product_name']),
                    pr['order_cnt']
                )
            self._logger.info(f"{datetime.utcnow()}: product cnt has been recorder into CDM")
                
        self._logger.info(f"{datetime.utcnow()}: FINISH")

