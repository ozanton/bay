-- orders view
CREATE or replace VIEW analysis.orders AS
SELECT *
FROM production.orders
where extract(year from order_ts::date) = '2022'
order by order_ts;

-- orderotems view
CREATE or replace VIEW analysis.orderitems AS
SELECT *
FROM production.orderitems
where order_id in (select order_id 
				   from production.orders 
				   where extract(year from order_ts::date) = '2022')
order by order_id;

-- orderstatuses
CREATE or replace VIEW analysis.orderstatuses AS
SELECT *
FROM production.orderstatuses;

-- products
CREATE or replace VIEW analysis.products AS
SELECT *
FROM production.products;

-- users view
CREATE or replace VIEW analysis.users AS
SELECT *
FROM production.users
where id in (select user_id 
				  from production.orders 
				  where extract(year from order_ts::date) = '2022');
