
drop table if exists STAGING.dialogs;
drop table if exists STAGING.group_log;
drop table if exists STAGING.groups;
drop table if exists STAGING.users;
create table STAGING.users
(
    id   int,
    chat_name varchar(200),
    registration_dt timestamp(0),
    country varchar(200),
    age int,
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
)
order by id; 


create table STAGING.groups
(
    id   int,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp(6),
    is_private boolean,
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED,
    constraint groups_users_fk_constraint foreign key (admin_id) references staging.users(id)
)
order by id, admin_id
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2); 


create table STAGING.dialogs
(
    message_id   int,
    message_ts timestamp,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int,
    CONSTRAINT C_PRIMARY PRIMARY KEY (message_id) DISABLED,
    constraint dialogs_message_from_fk foreign key (message_from) references STAGING.users(id),
    constraint dialogs_message_to_fk foreign key (message_to) references STAGING.users(id),
    constraint dialogs_message_group_fk foreign key (message_group) references STAGING.groups(id)
)
order by message_id
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
;

create table STAGING.group_log
(
    group_id   int,
    user_id int,
    user_id_from int,
    event varchar(6), 
    event_dt timestamp(0),
    CONSTRAINT C_PRIMARY PRIMARY KEY (group_id, user_id, event_dt) DISABLED,
    constraint group_log_users_fk_constraint foreign key (user_id) references staging.users(id),
    constraint group_log_groups_fk_constraint foreign key (group_id) references staging.groups(id)
)
order by group_id, user_id
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);