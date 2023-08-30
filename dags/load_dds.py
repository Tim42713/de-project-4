from datetime import datetime, timedelta
import time as time
import pandas as pd
import numpy as np
import psycopg2
import json as json
from wsgiref import headers
import requests as requests

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

pg_conn_1 = PostgresHook.get_connection('PG_WAREHOUSE_CONNECTION')

conn_1 = psycopg2.connect(
    f"""
    host='{pg_conn_1.host}'
    port='{pg_conn_1.port}'
    dbname='{pg_conn_1.schema}' 
    user='{pg_conn_1.login}' 
    password='{pg_conn_1.password}'
    """
    )   

def load_restaurants():
    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'restaurants'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name)
SELECT DISTINCT 
       json_array_elements(r.object_value::JSON)->>'_id'  AS restaurant_id,
       json_array_elements(r.object_value::JSON)->>'name' AS restaurant_name
FROM stg.restaurants AS r
INNER JOIN stg.settings AS s ON r.update_ts = s.workflow_key::timestamp
WHERE s.workflow_key::timestamp = (SELECT MAX (workflow_key::timestamp) FROM stg.settings WHERE workflow_settings = 'restaurants' )
ON conflict (restaurant_id) DO UPDATE 
SET 
    restaurant_id = excluded.restaurant_id,
    restaurant_name = excluded.restaurant_name;
    """

    postgres_insert_query_settings = """ 
        INSERT INTO dds.dm_settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(f_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

def load_couriers():
    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'couriers'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
INSERT INTO dds.dm_couriers(courier_id, courier_name)
SELECT DISTINCT
       json_array_elements(c.object_value::JSON)->>'_id'  AS courier_id,
       json_array_elements(c.object_value::JSON)->>'name' AS courier_name
FROM stg.couriers AS c
INNER JOIN stg.settings AS s ON c.update_ts = s.workflow_key::timestamp
WHERE s.workflow_key::timestamp = (SELECT MAX (workflow_key::timestamp) FROM stg.settings WHERE workflow_settings = 'couriers' )
ON conflict (courier_id) DO UPDATE
SET 
    courier_id = excluded.courier_id,
    courier_name = excluded.courier_name;
    """

    postgres_insert_query_settings = """ 
        INSERT INTO dds.dm_settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(f_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

def load_deliveries():
    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'deliveries'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
INSERT INTO dds.dm_deliveries (delivery_id, courier_id, order_id, address, delivery_ts, rate, tip_sum)
SELECT DISTINCT
       de.delivery_id, 
       c.id as courier_id_one, 
       o.id as order_id_one, 
       de.address, 
       de.delivery_ts, 
       de.rate, 
       de.tip_sum 
FROM ((SELECT json_array_elements(d.object_value::JSON) ->> 'delivery_id' as delivery_id,
              json_array_elements(d.object_value::JSON) ->> 'order_id' as order_id,
              json_array_elements(d.object_value::JSON) ->> 'courier_id' as courier_id,
              json_array_elements(d.object_value::JSON) ->> 'address' as address,
              (json_array_elements(d.object_value::JSON) ->> 'delivery_ts')::timestamp as delivery_ts,
              (json_array_elements(d.object_value::JSON) ->> 'rate')::int as rate,
              (json_array_elements(d.object_value::JSON) ->> 'tip_sum')::numeric(14,3) as tip_sum
       FROM stg.deliveries AS d
       INNER JOIN stg.settings AS s ON d.update_ts = s.workflow_key::timestamp
       WHERE s.workflow_key::timestamp = (SELECT MAX (workflow_key::timestamp) FROM stg.settings WHERE workflow_settings = 'deliveries'))) as de
INNER JOIN dds.dm_orders o ON de.order_id = o.order_id 
INNER JOIN dds.dm_couriers c ON de.courier_id = c.courier_id
ON conflict (delivery_id) do UPDATE 
SET 
    delivery_id = excluded.delivery_id,
    courier_id = excluded.courier_id,
    order_id = excluded.order_id,
    address = excluded.address,
    delivery_ts = excluded.delivery_ts,
    rate = excluded.rate,
    tip_sum = excluded.tip_sum;
    """

    postgres_insert_query_settings = """ 
        INSERT INTO dds.dm_settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(f_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

default_args = {
    'owner': 'Airflow',
    'schedule_interval':'@once',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup':False
}

with DAG(
        'loas_dds',
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['sprint5', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = EmptyOperator(task_id="start")
    with TaskGroup("load_dds_tables") as load_tables:
        t21 = PythonOperator(task_id="restaurants", python_callable=load_restaurants, dag=dag)
        t22 = PythonOperator(task_id="couriers", python_callable=load_couriers, dag=dag)
        t23 = PythonOperator(task_id="deliveries", python_callable=load_deliveries, dag=dag)
    t4 = EmptyOperator(task_id="end")
    
    t1  >> load_tables >> t4