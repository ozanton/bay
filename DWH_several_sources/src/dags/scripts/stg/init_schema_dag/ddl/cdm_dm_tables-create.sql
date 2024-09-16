-- dm_settlement_report
create table if not exists cdm.dm_settlement_report (
    id serial,
    restaurant_id varchar(100) NOT NULL,
    restaurant_name varchar(100) NOT NULL,
    settlement_date date NOT NULL,
    orders_count int NOT NULL DEFAULT 0,
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0, 
    orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
    orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
    restaurant_reward_sum numeric(14, 2) NOT null DEFAULT 0,
    constraint dm_settlement_report_pk primary key (id),
    constraint dm_settlement_report_settlement_date_check check
        (
        settlement_date::date >= '2022-01-01' 
        AND settlement_date::date < '2500-01-01'
        ),
    CONSTRAINT orders_count_value_check CHECK (orders_count >=0),
    CONSTRAINT orders_total_sum_value_check CHECK (orders_total_sum >=0),
    CONSTRAINT orders_bonus_payment_sum_value_check CHECK (orders_bonus_payment_sum >=0),
    CONSTRAINT orders_bonus_granted_sum_value_check CHECK (orders_bonus_granted_sum >=0),
    CONSTRAINT oorder_processing_fee_value_check CHECK (order_processing_fee >=0),
    CONSTRAINT restaurant_reward_sum_value_check CHECK (restaurant_reward_sum >=0),
    CONSTRAINT restaurant_id_unique_constraint UNIQUE (restaurant_id, settlement_date)    
);


-- dm_courier_ledger
create table if not exists cdm.dm_courier_ledger (
id smallint not null,
courier_id varchar(100) not null,
courier_name varchar(100) not null,
settlement_year smallint not null,
settlement_month smallint not null,
orders_count int not null default 0,
orders_total_sum numeric(14,2) not null default 0,
rate_avg numeric(2,1) not null default 0,
order_processing_fee numeric(14,2) not null default 0,
courier_order_sum numeric(14,2) not null default 0,
courier_tips_sum numeric(14,2) not null default 0,
courier_reward_sum numeric(14,2) not null default 0,
CONSTRAINT dm_courier_ledger_pk PRIMARY KEY (id),
CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year >= 2022 ::int) AND (settlement_year < 2500::int))),
CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1 ::int) AND (settlement_month <= 12::int))),
CONSTRAINT courier_processing_fee_value_check CHECK ((order_processing_fee >= (0)::numeric)),
CONSTRAINT courier_order_sum_value_check CHECK ((courier_order_sum >= (0)::numeric)),
CONSTRAINT courier_tips_sum_value_check CHECK ((courier_tips_sum >= (0)::numeric)),
constraint courier_reward_sum_value_check check ((courier_reward_sum >= (0)::numeric)), 
CONSTRAINT courier_orders_count_value_check CHECK ((orders_count >= 0)),
CONSTRAINT courier_orders_total_sum_value_check CHECK ((orders_total_sum >= (0)::numeric)),
CONSTRAINT courier_id_unique_constraint UNIQUE (courier_id, settlement_year, settlement_month)
);