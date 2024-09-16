delete from cdm.dm_settlement_report where settlement_date :: date = '{{ds}}'::date;

INSERT INTO cdm.dm_settlement_report
(restaurant_id,
restaurant_name,
settlement_date,
orders_count,
orders_total_sum,
orders_bonus_payment_sum,
orders_bonus_granted_sum,
order_processing_fee,
restaurant_reward_sum
)
select dr.id,
       dr.restaurant_name,
       dt.date :: date as settlement_date,
       count(distinct fps.order_id) as orders_count, 
       sum(fps.total_sum) as orders_total_sum,
       sum(fps.bonus_payment) as bonus_payment_sum,
       sum(fps.bonus_grant) as bonus_granted_sum,
       (sum(fps.total_sum) * 25) / 100 as order_processing_fee,
       sum(fps.total_sum) - (sum(fps.total_sum) * 25) / 100 - sum(fps.bonus_payment) as restaurant_reward_sum 
from dds.fct_product_sales as fps
left outer join dds.dm_orders as dmo on fps.order_id =dmo.id
left outer join dds.dm_products as dp on fps.product_id = dp.id
left outer join dds.dm_restaurants as dr on dp.restaurant_id = dr.id
left outer join dds.dm_timestamps as dt on dmo.timestamp_id = dt.id
where dmo.order_status = 'CLOSED' and dt.ts ::date = '{{ds}}'::date
group by dr.id, dr.restaurant_name , dt.date
ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET
id = EXCLUDED.id,
restaurant_id = EXCLUDED.id,
restaurant_name = EXCLUDED.restaurant_name,
orders_count = EXCLUDED.orders_count,
orders_total_sum = EXCLUDED.orders_total_sum,
orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
order_processing_fee = EXCLUDED.order_processing_fee,
restaurant_reward_sum = EXCLUDED.restaurant_reward_sum