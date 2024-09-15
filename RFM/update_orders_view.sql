CREATE or replace VIEW analysis.orders AS
with order_statuses as (SELECT 
			   t.order_id,
               t.status_id,
			   t.dttm as order_ts
            from (SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY order_id
                             ORDER BY dttm desc) row_num
                  FROM production.orderstatuslog) t
             WHERE row_num = 1)
SELECT p.order_id,
       os.order_ts,
	   user_id,
       bonus_payment,
       payment,
       cost,
       bonus_grant,
       os.status_id as status
FROM production.orders as p
left outer join order_statuses as os on p.order_id = os.order_id
where extract(year from os.order_ts::date) = '2022'
order by os.order_ts;