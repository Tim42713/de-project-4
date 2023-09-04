CREATE TABLE stg.restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT restaurants_pkey PRIMARY KEY (id),
	CONSTRAINT restaurants_restaurant_id_uniq UNIQUE (restaurant_id)
);

CREATE TABLE stg.couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT couriers_courier_id_uniq UNIQUE (courier_id),
	CONSTRAINT couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT deliveries_delivery_id_uniq UNIQUE (delivery_id),
	CONSTRAINT deliveries_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT settings_pkey PRIMARY KEY (id),
	CONSTRAINT settings_workflow_key_key UNIQUE (workflow_key)
);