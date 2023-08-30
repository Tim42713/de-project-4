CREATE TABLE IF NOT EXISTS stg.restaurants(
id serial PRIMARY KEY NOT NULL,
object_value text NOT NULL,
update_ts timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.couriers(
id serial PRIMARY KEY NOT NULL,
object_value text NOT NULL,
update_ts timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.deliveries(
id serial PRIMARY KEY NOT NULL,
object_value text NOT NULL,
update_ts timestamp NOT NULL
);
