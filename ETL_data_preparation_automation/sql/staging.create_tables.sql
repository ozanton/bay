
create table staging.customer_research (
    id serial,
    date_id timestamp,
    category_id int,
    geo_id int,
    sales_qty int,
    sales_amt numeric(14,2),
    constraint customer_research_pk primary key (id)
);

create table staging.user_activity_log (
    id serial,
    uniq_id varchar(100),
    date_time timestamp,
    action_id bigint,
    customer_id bigint,
    quantity bigint,
    constraint user_activity_log_pk primary key (id)
);

create table staging.user_order_log (
    id serial,
    uniq_id varchar(100), 
    date_time timestamp, 
    city_id int, 
    city_name varchar(100),
    customer_id bigint, 
    first_name varchar(100), 
    last_name varchar (100), 
    item_id int, 
    item_name varchar(100),
    quantity bigint, 
    payment_amount numeric(14,2),
    constraint user_order_log_pk primary key (id)
);

create table staging.price_log (
	id serial,
	category_name varchar(50),
	price numeric(10,2),
	constraint price_log_pk primary key (id)
);
