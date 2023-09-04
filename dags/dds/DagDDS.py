import os
import sys

import pendulum
from airflow.decorators import dag, task
from dds.GetDDS import StgDataLoader
from datetime import datetime

sys.path.append(os.path.realpath("../"))
from lib.pg_connect import ConnectionBuilder

@dag(
    'load_dds',
    schedule_interval='0/15 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['sprint5', 'example'],
    is_paused_upon_creation=True,
)

def dds_couriers_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="courier")
    def load_courier():
        courier_loader = StgDataLoader(dwh_connect)
        courier_loader.load_couriers()

    @task(task_id="restaurants")
    def load_restaurant():
        restaurants_loader = StgDataLoader(dwh_connect)
        restaurants_loader.load_restaurants()
    
    @task(task_id="deliveries")
    def load_delivery():
        delivery_loader = StgDataLoader(dwh_connect)
        delivery_loader.load_deliveries()

    load_dict_couriers = load_courier()
    load_dict_couriers
    load_dict_restaurant = load_restaurant()
    load_dict_restaurant
    load_dict_delivery = load_delivery()
    load_dict_delivery 

dds_couriers_dag = dds_couriers_dag()