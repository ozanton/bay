delete from cdm.dm_courier_ledger where settlement_year = date_part('year', '{{ds}}'::date) and
	settlement_month = date_part('month', '{{ds}}'::date);

with avg_rating as (select dc.id,
						   avg(rate::float) as avg_r 
				    from dds.dm_orders do2 
				    inner join dds.dm_couriers dc on do2.courier_id = dc.id 
				    group by dc.id),
     r_sum as (select case 
	   				    when ar.avg_r < 4 then order_sum*0.05
					   	when ar.avg_r >= 4 and rate < 4.5 then order_sum*0.07
					   	when ar.avg_r >= 4.5 and rate < 4.9 then order_sum*0.08
					   	when ar.avg_r >= 4.9 then order_sum* 0.1
	 				  end as r_sum,
	 				  do2.id as order_id,
	 				  ar.avg_r as avg_rate
			   from dds.dm_orders do2 
			   inner join avg_rating ar on do2.courier_id =ar.id),
     rtbs_payment as (select order_id,
     				case 
     					when avg_rate < 4 and r_sum < 100 then 100
					   	when avg_rate >= 4 and avg_rate < 4.5 and r_sum < 150 then 150
					   	when avg_rate >= 4.5 and avg_rate < 4.9  and r_sum  < 170  then 170
					   	when avg_rate >= 4.9 and r_sum < 200 then 200
					   	else r_sum
     				end as rtbs_payment
     				from r_sum)
INSERT INTO cdm.dm_courier_ledger 
(id, courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum,
rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
select dc.id,
       dc.courier_id,
       dc.courier_name,
       dt."year" as settlement_year,
       dt."month" as settlement_month,
       count(do2.id) as orders_count,
       sum(do2.order_sum) as orders_total_sum,
       avg(do2.rate::float) as rate_avg,
       sum(do2.order_sum)*0.25 as order_processing_fee,
       sum(rp.rtbs_payment) as courier_payment_sum,
       sum(do2.tip_sum) as courier_tips_sum,
       sum(rp.rtbs_payment) + sum(do2.tip_sum) * 0.95 as courier_reward_sum
from dds.dm_orders as do2
inner join dds.dm_couriers as dc on do2.courier_id = dc.id
inner join dds.dm_timestamps as dt on do2.timestamp_id = dt.id
left join rtbs_payment as rp on rp.order_id = do2.id
where dt."year" = date_part('year', '{{ds}}'::date) and
    dt."month" = date_part('month', '{{ds}}'::date)
group by dc.id, dc.courier_id, dc.courier_name, dt."year", dt."month"
ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE SET
id = EXCLUDED.id,
courier_id = EXCLUDED.courier_id,
courier_name = EXCLUDED.courier_name,
settlement_year = EXCLUDED.settlement_year,
settlement_month = EXCLUDED.settlement_month,
orders_count = EXCLUDED.orders_count,
orders_total_sum = EXCLUDED.orders_total_sum,
rate_avg = EXCLUDED.rate_avg,
order_processing_fee = EXCLUDED.order_processing_fee,
courier_order_sum = EXCLUDED.courier_order_sum,
courier_tips_sum = EXCLUDED.courier_tips_sum,
courier_reward_sum = EXCLUDED.courier_reward_sum;



