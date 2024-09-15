
-- mart.d_city definition

CREATE TABLE mart.d_city (
	id serial4 NOT NULL,
	city_id int4 NULL,
	city_name varchar(50) NULL,
	CONSTRAINT d_city_city_id_key UNIQUE (city_id),
	CONSTRAINT d_city_pkey PRIMARY KEY (id)
);
CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);

-- mart.d_customer definition

CREATE TABLE mart.d_customer (
	id serial4 NOT NULL,
	customer_id int4 NOT NULL,
	first_name varchar(15) NULL,
	last_name varchar(15) NULL,
	city_id int4 NULL,
	CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id),
	CONSTRAINT d_customer_pkey PRIMARY KEY (id)
);
CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);

-- mart.d_item definition

CREATE TABLE mart.d_item (
	id serial4 NOT NULL,
	item_id int4 NOT NULL,
	item_name varchar(50) NULL,
	CONSTRAINT d_item_item_id_key UNIQUE (item_id),
	CONSTRAINT d_item_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);

-- mart.f_sales definition

CREATE TABLE mart.f_sales (
	id serial4 NOT NULL,
	date_id int4 NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT f_sales_pkey PRIMARY KEY (id)
);
CREATE INDEX f_ds1 ON mart.f_sales USING btree (date_id);
CREATE INDEX f_ds2 ON mart.f_sales USING btree (item_id);
CREATE INDEX f_ds3 ON mart.f_sales USING btree (customer_id);
CREATE INDEX f_ds4 ON mart.f_sales USING btree (city_id);


-- mart.f_sales foreign keys

ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_item_id_fkey1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);


--mart.f_customer_retention

create table mart.f_customer_retention (
id serial,
new_customers_count int,
returned_customers_count int,
refunded_customer_count int,
period_name varchar(6),
period_id int,
item_id int,
new_customers_revenue numeric(10,2),
returning_customers_revenue numeric(10,2),
customers_refunded int,
constraint mart_f_customer_retention_pk primary key (id)
);