CREATE TABLE IF NOT EXISTS dds.dm_restaurants(
	id serial NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	CONSTRAINT dds_dm_restaurants_pkey PRIMARY KEY(id),
    CONSTRAINT dds_dm_restaurants_index UNIQUE(restaurant_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers(
	id serial NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dds_dm_couriers_pkey PRIMARY KEY(id),
    CONSTRAINT dds_dm_couriers_index UNIQUE(courier_id)
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
	id serial NOT NULL,
	delivery_id varchar NOT NULL,
	order_id int NOT NULL,
	courier_id int NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum int NOT NULL,
	tip_sum int NOT NULL,
	CONSTRAINT dds_dm_deliveries_pkey PRIMARY KEY(id),
	CONSTRAINT dds_dm_deliveries_index UNIQUE(delivery_id),
	CONSTRAINT dds_dm_deliveries_check CHECK((tip_sum >= 0)),
);

CREATE TABLE dds.dm_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT settings_pkey PRIMARY KEY (id),
	CONSTRAINT settings_workflow_key_key UNIQUE (workflow_key)
);

ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_courier_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_timestamps_fkey FOREIGN KEY (delivery_ts) REFERENCES dds.dm_timestamps(ts);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_orders_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);