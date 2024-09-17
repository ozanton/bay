from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository, OrderDdsBuilder
import uuid
import json


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 dds_order_builder: OrderDdsBuilder,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._dds_order_builder = dds_order_builder
        self._logger = logger
        self._batch_size = batch_size



    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")
            
            payload = self._dds_order_builder(msg['payload'])
            
            self._dds_repository.h_user_insert(payload.h_user())
            
            for h_product in payload.h_product():
                self._dds_repository.h_product_insert(h_product)

            self._dds_repository.h_order_insert(payload.h_order())
            
            for h_category in payload.h_category():
                self._dds_repository.h_category_insert(h_category)

            self._dds_repository.h_restaurant_insert(payload.h_restaurant())

            self._dds_repository.l_order_user_insert(payload.l_order_user())

            for l_op in payload.l_order_product():
                self._dds_repository.l_order_product_insert(l_op)

            for l_pc in payload.l_product_category():
                self._dds_repository.l_product_category_insert(l_pc)

            for l_pr in payload.l_product_restaurant():
                self._dds_repository.l_product_restaurant_insert(l_pr)

            self._dds_repository.s_user_names_insert(payload.s_user_names())

            self._dds_repository.s_restaurant_names_insert(payload.s_restaurant_names())

            self._dds_repository.s_order_cost_insert(payload.s_order_cost())

            self._dds_repository.s_order_status_insert(payload.s_order_status())

            for s_pn in payload.s_product_names():
                self._dds_repository.s_product_names_insert(s_pn)

            
            cat_cnt = self._dds_repository.cdm_get_categories_cnt()
            prod_cnt = self._dds_repository.cdm_get_products_cnt()

            dst_msg = {
                "users_categories": self._format_cat_cnt(cat_cnt),
                "users_products" :self._format_prod_cnt(prod_cnt)
                }
            
            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")


        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _generate_uuid(self, id, name):
        namespace = uuid.NAMESPACE_DNS
        combined_string = str(id) + str(name)
        return str(uuid.uuid5(namespace, combined_string))

    def _format_cat_cnt(self, li):
        lst = []
        for el in li:
            res = {'user_id': el[0],
                  'category_id': el[1],
                  'category_name': el[2],
                  'order_cnt':el[3]}
            lst.append(res)
        return lst
    
    def _format_prod_cnt(self, li):
        lst = []
        for el in li:
            res = {'user_id': el[0],
                  'product_id': el[1],
                  'product_name': el[2],
                  'order_cnt':el[3]}
            lst.append(res)
        return lst

