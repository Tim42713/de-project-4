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

def load_dm_courier_ledger():
    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'courier_ledger'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
SELECT c.courier_id,
       c.courier_name,
       t.year,
       t.month,
       COUNT(d.delivery_id) AS orders_count,
       SUM(o."sum") AS orders_total_sum,
       AVG(d.rate)::float AS rate_avg,
       SUM(o."sum") * 0.25 AS order_processing_fee,
       CASE
           WHEN AVG(d.rate) < 4 THEN (CASE WHEN 0.05 * SUM(o."sum") <= 100 THEN 100 ELSE 0.05 * SUM(o."sum") END)
           WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN (CASE WHEN 0.07 * SUM(o."sum") <= 150 THEN 150 ELSE 0.07 * SUM(o."sum") END)
           WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN (CASE WHEN 0.08 * SUM(o."sum") <= 175 THEN 175 ELSE 0.08 * SUM(o."sum") END)
           WHEN AVG(d.rate) >= 4.9 THEN (CASE WHEN 0.1 * SUM(o."sum") <= 200 THEN 200 ELSE 0.1 * SUM(o."sum") END)
       END AS courier_order_sum,
       SUM(d.tip_sum) AS courier_tips_sum,
       (CASE
           WHEN AVG(d.rate) < 4 THEN (CASE WHEN 0.05 * SUM(o."sum") <= 100 THEN 100 ELSE 0.05 * SUM(o."sum") END)
           WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN (CASE WHEN 0.07 * SUM(o."sum") <= 150 THEN 150 ELSE 0.07 * SUM(o."sum") END)
           WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN (CASE WHEN 0.08 * SUM(o."sum") <= 175 THEN 175 ELSE 0.08 * SUM(o."sum") END)
           WHEN AVG(d.rate) >= 4.9 THEN (CASE WHEN 0.1 * SUM(o."sum") <= 200 THEN 200 ELSE 0.1 * SUM(o."sum") END)
       END + 0.95 * SUM(d.tip_sum)) AS courier_reward_sum
FROM dds.dm_deliveries AS d
INNER JOIN dds.dm_couriers AS c ON d.courier_id = c.id
INNER JOIN dds.dm_timestamps AS t ON d.delivery_ts = t.ts
INNER JOIN dds.dm_orders AS o ON d.order_id = o.id
GROUP BY c.courier_id, c.courier_name, t.year, t.month
ON CONFLICT (courier_id, settlement_year, settlement_month) do UPDATE
SET courier_name = excluded.courier_name,
    orders_count = excluded.orders_count,
    orders_total_sum = excluded.orders_total_sum,
    rate_avg = excluded.rate_avg,
    order_processing_fee = excluded.order_processing_fee,
    courier_order_sum = excluded.courier_order_sum,
    courier_tips_sum = excluded.courier_tips_sum,
    courier_reward_sum = excluded.courier_reward_sum;
    """

    postgres_insert_query_settings = """ 
        INSERT INTO cdm.settings (workflow_key, workflow_settings) 
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
        'load_cdm',
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['sprint5', 'example'],
) as dag:

    t1 = EmptyOperator(task_id="start")
    with TaskGroup("load_cdm_tables") as load_tables:
        t21 = PythonOperator(task_id="courier_ledger", python_callable=load_dm_courier_ledger, dag=dag)
    t4 = EmptyOperator(task_id="end")
    
    t1 >> load_tables >> t4