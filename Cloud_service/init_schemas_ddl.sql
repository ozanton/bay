drop table if exists cdm.user_product_counters;
create table if not exists cdm.user_product_counters (
id serial not null,
user_id uuid not null,
product_id uuid not null,
product_name varchar not null,
order_cnt int not null,
constraint user_product_countrs primary key (id),
constraint user_product_order_cnt_check CHECK (order_cnt > 0)
);

drop table if exists cdm.user_category_counters;
create table if not exists cdm.user_category_counters (
id serial not null,
user_id uuid not null,
category_id uuid not null,
category_name varchar not null,
order_cnt int not null,
constraint user_categlry_countrs primary key (id),
constraint category_order_cnt_check CHECK (order_cnt > 0)
);


drop table if exists stg.order_events;
create table if not exists stg.order_events (
id serial not null,
object_id int not null,
object_type varchar not null,
sent_dttm timestamp not null,
payload json not null,
constraint order_events_id primary key (id),
constraint object_id_unique unique (object_id)
);


drop table if exists dds.h_user;
create table if not exists dds.h_user (
h_user_pk UUID not null,
user_id varchar not null,
load_dt timestamp not null,
load_src varchar not null,
constraint h_user_h_user_pk primary key (h_user_pk)
);


drop table if exists dds.h_product;
create table if not exists dds.h_product (
h_product_pk UUID not null,
product_id varchar not null,
load_dt timestamp not null,
load_src varchar not null,
constraint h_user_h_product_pk primary key (h_product_pk)
);


drop table if exists dds.h_category;
create table if not exists dds.h_category (
h_category_pk UUID not null,
category_name varchar not null,
load_dt timestamp not null,
load_src varchar not null,
constraint h_user_h_category_pk primary key (h_category_pk)
);


drop table if exists dds.h_restaurant;
create table if not exists dds.h_restaurant (
h_restaurant_pk UUID not null,
restaurant_id varchar not null,
load_dt timestamp not null,
load_src varchar not null,
constraint h_user_h_restaurant_pk primary key (h_restaurant_pk)
);


drop table if exists dds.h_order;
create table if not exists dds.h_order (
h_order_pk UUID not null,
order_id int not null,
order_dt timestamp not null,
load_dt timestamp not null,
load_src varchar not null,
constraint h_user_h_order_pk primary key (h_order_pk)
);


drop table if exists dds.l_order_product;
create table if not exists dds.l_order_product (
hk_order_product_pk UUID not null,
h_product_pk UUID not null,
h_order_pk UUID not null,
load_dt timestamp not null,
load_src varchar not null,
constraint hk_order_product_pk_pk primary key (hk_order_product_pk),
constraint l_order_product_h_product_fk foreign key (h_product_pk) references dds.h_product(h_product_pk),
constraint l_order_product_h_order_fk foreign key (h_order_pk) references dds.h_order(h_order_pk)
);


drop table if exists dds.l_product_restaurant;
create table if not exists dds.l_product_restaurant (
hk_product_restaurant_pk UUID not null,
h_product_pk UUID not null,
h_restaurant_pk UUID not null,
load_dt timestamp not null,
load_src varchar not null,
constraint l_product_restaurant_pk primary key (hk_product_restaurant_pk),
constraint l_product_restaurant_h_product_fk foreign key (h_product_pk) references dds.h_product(h_product_pk),
constraint l_product_restaurant_h_restaurant_fk foreign key (h_restaurant_pk) references dds.h_restaurant(h_restaurant_pk)
);


drop table if exists dds.l_product_category;
create table if not exists dds.l_product_category (
hk_product_category_pk UUID not null,
h_product_pk UUID not null,
h_category_pk UUID not null,
load_dt timestamp not null,
load_src varchar not null,
constraint l_product_category_pk primary key (hk_product_category_pk),
constraint l_product_category_h_product_fk foreign key (h_product_pk) references dds.h_product(h_product_pk),
constraint l_product_category_h_category_fk foreign key (h_category_pk) references dds.h_category(h_category_pk)
);


drop table if exists dds.l_order_user;
create table if not exists dds.l_order_user (
hk_order_user_pk UUID not null,
h_order_pk UUID not null,
h_user_pk UUID not null,
load_dt timestamp not null,
load_src varchar not null,
constraint l_order_user_pk primary key (hk_order_user_pk),
constraint l_order_user_h_order_fk foreign key (h_order_pk) references dds.h_order(h_order_pk),
constraint l_order_user_h_user_fk foreign key (h_user_pk) references dds.h_user(h_user_pk)
);

drop table if exists dds.s_user_names;
create table if not exists dds.s_user_names (
h_user_pk UUID not null,
username varchar not null,
userlogin varchar not null,
load_dt timestamp not null,
load_src varchar not null,
hk_user_names_hashdiff UUID not null,
constraint s_user_pk_load_dt_pk primary key (h_user_pk, load_dt),
constraint s_user_names_user_fk foreign key (h_user_pk) references dds.h_user(h_user_pk)
);


drop table if exists dds.s_product_names;
create table if not exists dds.s_product_names (
h_product_pk UUID not null,
"name" varchar not null,
load_dt timestamp not null,
load_src varchar not null,
hk_product_names_hashdiff UUID not null,
constraint s_product_pk_load_dt_pk primary key (h_product_pk, load_dt),
constraint s_product_names_product_fk foreign key (h_product_pk) references dds.h_product(h_product_pk)
);


drop table if exists dds.s_restaurant_names;
create table if not exists dds.s_restaurant_names (
h_restaurant_pk UUID not null,
"name" varchar not null,
load_dt timestamp not null,
load_src varchar not null,
hk_restaurant_names_hashdiff UUID not null,
constraint s_restaurant_pk_load_dt_pk primary key (h_restaurant_pk, load_dt),
constraint s_restaurant_names_restaurant_fk foreign key (h_restaurant_pk) references dds.h_restaurant(h_restaurant_pk)
);


drop table if exists dds.s_order_cost;
create table if not exists dds.s_order_cost (
h_order_pk UUID not null,
"cost" decimal(19,5) not null,
payment decimal(19,5) not null,
load_dt timestamp not null,
load_src varchar not null,
hk_order_cost_hashdiff UUID not null,
constraint s_odrer_cost_load_dt_pk primary key (h_order_pk, load_dt),
constraint s_order_cost_order_fk foreign key (h_order_pk) references dds.h_order(h_order_pk),
constraint s_order_cost_check check ("cost" >= 0),
constraint s_order_cost_payment check (payment >= 0)
);

drop table if exists dds.s_order_status;
create table if not exists dds.s_order_status (
h_order_pk UUID not null,
status varchar not null,
load_dt timestamp not null,
load_src varchar not null,
hk_order_status_hashdiff UUID not null,
constraint s_order_status_pk_load_dt_pk primary key (h_order_pk, load_dt),
constraint s_order_status_order_fk foreign key (h_order_pk) references dds.h_order(h_order_pk)
);