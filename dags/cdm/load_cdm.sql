CREATE TABLE cdm.dm_courier_ledger (
	id serial NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar(100) NOT NULL,
    settlement_year int NOT NULL,
    settlement_month int NOT NULL,
    orders_count int NOT NULL,
    orders_total_sum numeric(14, 2) NOT NULL,
    rate_avg numeric(2, 1) NOT NULL,
    order_processing_fee numeric(14, 2) NOT NULL,
    courier_order_sum numeric(14, 2) NOT NULL,
    courier_tips_sum numeric(14, 2) NOT NULL,
    courier_reward_sum numeric(14, 2) NOT NULL,
    CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
    CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
    CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year >= 2022) AND (settlement_year < 2500))),
    CONSTRAINT dm_courier_ledger_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= 0)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= 0)),
    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= 0)),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= 0)),
    CONSTRAINT dm_courier_ledger_courier_tips_reward_check CHECK ((courier_reward_sum >= 0))
);

ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_uni UNIQUE (courier_id, settlement_year, settlement_month);	
	
	
	
	
	
	
    





	
	
	
	
	
	
	

	
	


