-- dm_users
create table IF NOT EXISTS dds.dm_users(
    id serial not null,
    user_id varchar not null,
    user_name varchar not null,
    user_login varchar not null,
    constraint dm_users_pkey primary key (id)
);

-- dm_restaurants
create table IF NOT EXISTS dds.dm_restaurants ( 
    id serial not null constraint dm_restaurants_pkey primary key,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    active_from timestamp not null,
    active_to timestamp not null);

-- dm_products
create table IF NOT EXISTS dds.dm_products ( 
    id serial not null constraint dm_products_pkey primary key,
    restaurant_id int not null,
    product_id varchar not null,
    product_name varchar not null,
    product_price numeric(14, 2) not null default 0 constraint dm_product_price_check check (product_price >=0),
    active_from timestamp not null,
    active_to timestamp not null,
    constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants
);

-- dm_timestamps
create table IF NOT EXISTS dds.dm_timestamps(
    id serial not null constraint dm_timestamps_pk primary key,
    ts timestamp not null,
    year smallint not null constraint dm_timestamps_year_check check (year >= 2022 and year < 2500),
    month smallint not null constraint dm_timestamps_month_check check (month >= 1 and month <= 12),
    day smallint not null constraint dm_timestamps_day_check check (day >= 1 and day <= 31),
    "time" time not null,
    "date" date not null 
);

-- dm_orders
create table IF NOT EXISTS dds.dm_orders(
    id serial not null constraint dm_orders_pk primary key,
    user_id int not null,
    restaurant_id int not null,
    timestamp_id int not null,
    order_key varchar not null,
    order_status varchar not null,
    constraint dm_orders_user_id_fk foreign key (user_id) references dds.dm_users(id),
    constraint dm_orders_restaurant_id_fk foreign key(restaurant_id) references dds.dm_restaurants(id),
    constraint dm_orders_timestamp_id_fk foreign key(timestamp_id) references dds.dm_timestamps(id)
);

--dm_fct_sales
create table IF NOT EXISTS dds.fct_product_sales(
    id serial not null constraint fct_product_sales_id primary key,
    product_id int not null,
    order_id int not null,
    "count" int not null default 0 constraint fct_product_sales_count_check check (count >= 0),
    price numeric(14,2) not null default 0 constraint fct_product_sales_price_check check (price >= 0),
    total_sum numeric(14,2) not null default 0 constraint fct_product_sales_total_sum_check check (total_sum >= 0),
    bonus_payment numeric(14,2) not null default 0 constraint fct_product_sales_bonus_payment_check check (bonus_payment >= 0),
    bonus_grant numeric(14,2) not null default 0 constraint fct_product_sales_bonus_grant_check check (bonus_grant >= 0),
    constraint fct_product_sales_product_id_fk foreign key (product_id) references dds.dm_products,
    constraint fct_product_sales_order_id_fk foreign key (order_id) references dds.dm_orders
);

--dm_couriers
CREATE TABLE if not exists dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);


-- add courier_id, delivery_ts_id, order_sum, tip_sum, rate
alter table dds.dm_orders add column if not exists courier_id int;
alter table dds.dm_orders add column if not exists delivery_ts_id int;
alter table dds.dm_orders add column if not exists order_sum numeric(14,2);
alter table dds.dm_orders add column if not exists tip_sum numeric(14,2);
alter table dds.dm_orders add column if not exists rate smallint;