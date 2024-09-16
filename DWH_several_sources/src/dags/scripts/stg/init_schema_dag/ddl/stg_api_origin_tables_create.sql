CREATE TABLE if not exists stg.api_couriers (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_value text NOT NULL,
	update_ts timestamp,
	CONSTRAINT api_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT api_couriers_object_value_unique UNIQUE (object_value)
);

CREATE TABLE if not exists stg.api_restaurants (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_value text NOT NULL,
	update_ts timestamp,
	CONSTRAINT api_restaurants_pkey PRIMARY KEY (id),
	CONSTRAINT poi_restaurants_object_value_unique UNIQUE (object_value)
);

CREATE TABLE if not exists stg.api_deliveries (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_value text NOT NULL,
	update_ts timestamp,
	CONSTRAINT api_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT poi_deliveries_object_value_unique UNIQUE (object_value)
);