-- bonussystem_users
CREATE TABLE if not exists stg.bonussystem_users (
	id int4 not null, 
	order_user_id text not null,
	CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);

--bonussystem_ranks
CREATE TABLE if not exists stg.bonussystem_ranks (
	id int4 not null,
	"name" varchar(2048) not null,
	bonus_percent numeric(19, 5) not null,
	min_payment_threshold numeric(19, 5) not null,
	constraint bonussystem_ranks_pkey primary key (id)
);

--bonussystem_events
CREATE TABLE if not exists stg.bonussystem_events (
	id int4 not null,
	event_ts timestamp not null,
	event_type varchar not null,
	event_value text not null,
	CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);
CREATE INDEX if not exists idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);