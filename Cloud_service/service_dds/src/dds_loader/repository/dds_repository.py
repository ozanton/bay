import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 


class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str


class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str 


class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str 


class L_Order_User(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_Order_Product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_Product_Category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class S_User_Names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: uuid.UUID


class S_Restaurant_Names(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: uuid.UUID


class S_Order_Cost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: uuid.UUID


class S_Order_Status(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: uuid.UUID


class S_Product_Names(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: uuid.UUID
    


class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "kafka_stg-order-topic"
        self.order_ns_uuid = uuid.NAMESPACE_DNS
    
    
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def _uuid_2(self, obj: Any, obj2: Any) -> uuid.UUID:
        combined_string = str(obj) + str(obj2)
        return uuid.uuid5(namespace=self.order_ns_uuid, name=combined_string)

    
    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    

    def h_product(self) -> List[H_Product]:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products
    

    def h_order(self) -> H_Order:
        order_id = self._dict['id']
        order_dt = self._dict['date']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    

    def h_category(self) -> List[H_Category]:
        categories = []
        for cat_dict in self._dict['products']:
            cat_name = cat_dict['category']
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(cat_name),
                    category_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return categories
    

    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._dict['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    

    def l_order_user(self) -> L_Order_User:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']
        return L_Order_User(
            hk_order_user_pk=self._uuid_2(order_id, user_id),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )  


    def l_order_product(self) -> L_Order_Product:
        order_id = self._dict['id']
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                L_Order_Product(
                    hk_order_product_pk=self._uuid_2(order_id, prod_id),
                    h_product_pk=self._uuid(prod_id),
                    h_order_pk=self._uuid(order_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products
    

    def l_product_category(self) -> L_Product_Category:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            cat_name = prod_dict['category']
            products.append(
                L_Product_Category(
                    hk_product_category_pk=self._uuid_2(prod_id, cat_name),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(cat_name),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products
    

    def l_product_restaurant(self) -> L_Product_Restaurant:
        products = []
        restaurant_id = self._dict['restaurant']['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk=self._uuid_2(prod_id, restaurant_id),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products
    

    def s_user_names(self) -> S_User_Names:
        user_id = self._dict['user']['id']
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        hk_user_names_hashdiff = self._uuid_2(user_id, username)
        return S_User_Names(
            h_user_pk=self._uuid(user_id),
            username= username,
            userlogin = userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff = hk_user_names_hashdiff
        )


    def s_restaurant_names(self) -> S_Restaurant_Names:
        restaurant_id = self._dict['restaurant']['id']
        name = self._dict['restaurant']['name']
        hk_user_restaurant_hashdiff = self._uuid_2(restaurant_id, name)
        return S_Restaurant_Names(
            h_restaurant_pk=self._uuid(restaurant_id),
            name= name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff = hk_user_restaurant_hashdiff

        )        
    

    def s_order_cost(self) -> S_Order_Cost:
        order_id = self._dict['id']
        cost = self._dict['cost']
        payment = self._dict['payment']
        hk_order_cost_hashdiff = self._uuid_2(order_id, cost)
        return S_Order_Cost(
            h_order_pk=self._uuid(order_id),
            cost= cost,
            payment=payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff = hk_order_cost_hashdiff

        )
    

    def s_order_status(self) -> S_Order_Status:
        order_id = self._dict['id']
        status = self._dict['status']
        hk_order_status_hashdiff = self._uuid_2(order_id, status)
        return S_Order_Status(
            h_order_pk=self._uuid(order_id),
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff = hk_order_status_hashdiff

        )
    

    def s_product_names(self) -> S_Product_Names:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            hk_product_names_hashdiff = self._uuid_2(prod_id, prod_name)
            products.append(
                S_Product_Names(
                    h_product_pk=self._uuid(prod_id),
                    name=prod_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=hk_product_names_hashdiff
                )
            )
        return products


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, obj: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'user_id': obj.user_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )


    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 


    def h_order_insert(self, obj: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
         
         
    def h_category_insert(self, obj: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )       


    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 


    def l_order_user_insert(self, obj: L_Order_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 


    def l_order_product_insert(self, obj: L_Order_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_product_pk,
                            h_order_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_product_pk)s,
                            %(h_order_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': obj.hk_order_product_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_order_pk': obj.h_order_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 


    def l_product_category_insert(self, obj: L_Product_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_product_pk,
                            h_category_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_product_pk)s,
                            %(h_category_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': obj.hk_product_category_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_category_pk': obj.h_category_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )


    def l_product_restaurant_insert(self, obj: L_Product_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(h_restaurant_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )


    def s_user_names_insert(self, obj: S_User_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (h_user_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'username': obj.username,
                        'userlogin': obj.userlogin,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_user_names_hashdiff': obj.hk_user_names_hashdiff
                    }
                )


    def s_restaurant_names_insert(self, obj: S_Restaurant_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                )


    def s_order_cost_insert(self, obj: S_Order_Cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff
                    }
                )


    def s_order_status_insert(self, obj: S_Order_Status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                    }
                )


    def s_product_names_insert(self, obj: S_Product_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                    }
                )


    def cdm_get_categories_cnt(self) -> None:
    
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    select cast(hu.h_user_pk as varchar) as user_id , cast(hc.h_category_pk as varchar) as category_id, hc.category_name, count(ho.h_order_pk) as order_cnt
                    from dds.h_user hu 
                    join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk 
                    join dds.h_order ho on lou.h_order_pk = ho.h_order_pk 
                    join dds.l_order_product lop on ho.h_order_pk = lop.h_order_pk 
                    join dds.h_product hp on lop.h_product_pk = hp.h_product_pk
                    join dds.l_product_category lpc on hp.h_product_pk = lpc.h_product_pk 
                    join dds.h_category hc on lpc.h_category_pk = hc.h_category_pk
                    join dds.s_order_status sos on ho.h_order_pk = sos.h_order_pk 
                    where sos.status = 'CLOSED'
                    group by hu.h_user_pk , hc.h_category_pk , hc.category_name;
                """
                )
                return cur.fetchall()
            

    def cdm_get_products_cnt(self) -> None:
    
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    select cast(hu.h_user_pk as varchar) as user_id, cast(hp.h_product_pk  as varchar) as product_id, spn."name" as product_name, count(ho.h_order_pk) as order_cnt
                    from dds.h_user hu 
                    join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk 
                    join dds.h_order ho on lou.h_order_pk = ho.h_order_pk 
                    join dds.l_order_product lop on ho.h_order_pk = lop.h_order_pk 
                    join dds.h_product hp on lop.h_product_pk = hp.h_product_pk
                    join dds.s_product_names spn on hp.h_product_pk = spn.h_product_pk 
                    join dds.s_order_status sos on ho.h_order_pk = sos.h_order_pk 
                    where sos.status = 'CLOSED'
                    group by hu.h_user_pk , hp.h_product_pk , spn."name";
                """
                )
                return cur.fetchall() 