drop table if exists DWH.s_auth_history;
drop table if exists DWH.s_dialog_info;
drop table if exists DWH.s_group_name;
drop table if exists DWH.s_group_private_status;
drop table if exists DWH.s_user_socdem;
drop table if exists DWH.s_user_chatinfo;
drop table if exists DWH.s_admins;
drop table if exists DWH.l_user_group_activity;
drop table if exists DWH.l_groups_dialogs;
drop table if exists DWH.l_admins;
drop table if exists DWH.l_user_message;
drop table if exists DWH.h_groups;
drop table if exists DWH.h_dialogs;
drop table if exists DWH.h_users;

-- creating the hubs

-- h_users
create table DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
; 

-- h_dialogs
create table DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      int,
    message_ts datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
; 

-- h_groups
create table DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
; 


-- creating link tables
-- l_user_message
create table DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 

-- l_admins
create table DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_admin_user REFERENCES DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_user_group_group REFERENCES DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- l_groups_dialogs
create table DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message_message REFERENCES DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_group_dialogs_group_group REFERENCES DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 

-- l_user_group_activity
create table DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_users_user REFERENCES DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group_group REFERENCES DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 

-- creating the satellite tables
-- s_admins
create table DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- s_user_chatinfo
create table DWH.s_user_chatinfo
(
hk_user_id bigint not null CONSTRAINT fk_s_chatinfo_h_users REFERENCES DWH.h_users (hk_user_id),
chat_name varchar(200),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- s_user_socdem
create table DWH.s_user_socdem
(
hk_user_id bigint not null CONSTRAINT fk_s_socdem_h_users REFERENCES DWH.h_users (hk_user_id),
country varchar(200),
age int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- s_group_private_status
create table DWH.s_group_private_status
(
hk_group_id bigint not null CONSTRAINT fk_s_group_private_status_h_groups REFERENCES DWH.h_groups (hk_group_id),
is_private boolean,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- s_group_name
create table DWH.s_group_name
(
hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES DWH.h_groups (hk_group_id),
group_name varchar(200),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- s_dialog_info
create table DWH.s_dialog_info
(
hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES DWH.h_dialogs (hk_message_id),
message varchar(1000),
message_from int,
message_to int,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- s_auth_history
create table DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_hk_user_group_activity REFERENCES DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
"event" varchar(6),
event_dt timestamp(0),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);