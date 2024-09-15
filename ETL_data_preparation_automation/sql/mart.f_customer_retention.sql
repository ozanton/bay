--mart.f_customer_retention

-- idempotency
delete from mart.f_customer_retention
where period_id = date_part('week', '{{ds}}'::date);

with wow as(
			SELECT *
			FROM 
				mart.f_sales fs2 
			WHERE date_trunc('week', to_date(date_id:: text, 'YYYYMMDD'))::date  = date_trunc('week', '{{ds}}'::date)::date
			),
new_customers as (select item_id, 
						 count(customer_id) as customers_count,
						 sum(payment_amount) as new_customers_revenue
				  from wow 
				  where customer_id in (
   					select customer_id from (select customer_id, count(id) as orders_count 
   											 from wow fs2 group by customer_id ) as sub 
   											 where orders_count = 1)
   				  group by item_id),
returned_customers as (select item_id,
                              count(distinct customer_id) as customers_count,
                              sum(payment_amount) as returning_customers_revenue 
                       from wow where customer_id in (
   					      select customer_id from (select customer_id, count(id) as orders_count from wow fs2 group by customer_id ) as sub 
   						  where orders_count > 1)
   					   group by item_id),
refunded_customers as (select item_id,count(customer_id) as customers_count
	  		  from wow
	  		  where status = 'refunded'
	  		  group by item_id),
refunded_items as (select item_id,
      				      count(id) as items_count
				   from wow
				   where status = 'refunded'
				   group by item_id)
insert into mart.f_customer_retention
(new_customers_count, returned_customers_count,refunded_customer_count,
period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue,
customers_refunded)
select 
       nc.customers_count as new_customers_count,
       rc.customers_count as returned_customers_count,
       ref_c.customers_count as refunded_customer_count,
       'weekly' as period_name,
       (select distinct date_part('week', to_date(date_id::text, 'YYYYMMDD')::date)
	    from wow) as period_id,
	    nc.item_id,
	    nc.new_customers_revenue,
	    rc.returning_customers_revenue,
	    ri.items_count as customers_refunded
from new_customers as nc
left join returned_customers as rc on nc.item_id=rc.item_id
left join refunded_customers as ref_c on nc.item_id=ref_c.item_id
left join refunded_items as ri on nc.item_id=ri.item_id;