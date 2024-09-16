--couriers_id
update dds.dm_orders as do3 
set
    courier_id = c.id
from (select do2.order_key,
             dc.id 
      from stg.api_deliveries ad
      inner join dds.dm_orders do2 on do2.order_key =  (ad.object_value :: JSON ->> 'order_id')
      inner join dds.dm_couriers dc on dc.courier_id = (ad.object_value :: JSON ->> 'courier_id')  
      ) as c 
where c.order_key = do3.order_key;


--delivery_ts_id
update dds.dm_orders as do3 
set
    delivery_ts_id = t.id
from (select dt.id,
             do2.order_key 
      from stg.api_deliveries ad
      inner join dds.dm_orders do2 on do2.order_key =  (ad.object_value :: JSON ->> 'order_id')
      inner join dds.dm_timestamps dt on dt.ts =  date_trunc('second', (object_value::JSON->>'delivery_ts')::timestamp)
      where dt.ts :: date = '{{ds}}'::date  
      ) as t 
where t.order_key = do3.order_key;


--sum
update dds.dm_orders as do3 
set
    order_sum = d.order_sum
from (select do2.order_key,
       (ad.object_value :: JSON ->> 'sum')  :: numeric(14,2) as order_sum       
      from stg.api_deliveries ad
      inner join dds.dm_orders do2 on do2.order_key =  (ad.object_value :: JSON ->> 'order_id')  
      where (ad.object_value::JSON->>'order_ts')::date = '{{ds}}'::date
      ) as d 
where d.order_key = do3.order_key;

--tip_sum
update dds.dm_orders as do3 
set
    tip_sum = d.tip_sum
from (select do2.order_key,
       (ad.object_value :: JSON ->> 'tip_sum')  :: numeric(14,2) as tip_sum       
      from stg.api_deliveries ad
      inner join dds.dm_orders do2 on do2.order_key =  (ad.object_value :: JSON ->> 'order_id')
      where (ad.object_value::JSON->>'order_ts')::date = '{{ds}}'::date  
      ) as d 
where d.order_key = do3.order_key;


--rate
update dds.dm_orders as do3 
set
    rate = d.rate
from (select do2.order_key,
       (ad.object_value :: JSON ->> 'rate') :: smallint as rate       
      from stg.api_deliveries ad
      inner join dds.dm_orders do2 on do2.order_key =  (ad.object_value :: JSON ->> 'order_id')
      where (ad.object_value::JSON->>'order_ts')::date = '{{ds}}'::date  
      ) as d 
where d.order_key = do3.order_key;

--set fkeys
alter table dds.dm_orders drop constraint if exists dm_orders_courier_id;
alter table dds.dm_orders add constraint dm_orders_courier_id foreign key (courier_id) references dds.dm_couriers(id);

alter table dds.dm_orders drop constraint if exists dm_orders_delivery_ts_id;
alter table dds.dm_orders add constraint dm_orders_delivery_ts_id foreign key (delivery_ts_id) references dds.dm_timestamps(id);




