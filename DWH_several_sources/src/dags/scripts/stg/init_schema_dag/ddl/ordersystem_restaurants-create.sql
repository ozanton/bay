-- ordersystem restaurants
CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

-- ordersystem_users
CREATE TABLE if not exists stg.ordersystem_users (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

-- ordersystem_orders
CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);