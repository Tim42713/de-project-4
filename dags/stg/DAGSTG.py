import os
import sys
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pendulum as pendulum
import psycopg as psycopg
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from bson.objectid import ObjectId
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from requests import get

sys.path.append(os.path.realpath("../"))
from lib.pg_connect import ConnectionBuilder, PgConnect
from stg.GetSTG import LoadAPI

@dag(
    'load_stg', 
    schedule_interval='0/15 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['sprint5', 'example'],    
    is_paused_upon_creation=True,
)
def stg_deliveries_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="deliveries")
    def load_deliver():
        delivery_loader = LoadAPI(dwh_connect)
        delivery_loader.load_deliveries()

    @task(task_id="courier")
    def load_couriers():
        courier_loader = LoadAPI(dwh_connect)
        courier_loader.load_couriers()

    @task(task_id="restaurants")
    def load_rest():
        restaurants_loader = LoadAPI(dwh_connect)
        restaurants_loader.load_rest()

    load_couriers = load_couriers()
    load_rest = load_rest()
    load_deliver = load_deliver()
    load_couriers
    load_rest
    load_deliver

stg_deliveries_dag = stg_deliveries_dag()