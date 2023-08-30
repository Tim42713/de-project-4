from datetime import datetime, timedelta
import time as time
import pandas as pd
import numpy as np
import psycopg2
import json
from wsgiref import headers
import requests

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

sort_field = "id"
sort_direction = "asc"
limit = 50
offset = 0
nickname = "timur_bynin"
cohort = "15"
api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
url_restaurants = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants" 
url_couriers = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers" 
url_deliveries = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries" 

class params:
    headers = {
        "X-Nickname" : nickname,
        "X-Cohort" : cohort,
        "X-API-KEY" : api_key
        }
    params = {
        "sort_field" : sort_field, 
        "sort_direction" : sort_direction,
        "limit" : limit,
        "offset" : offset
        }

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
    r = requests.get(
        url = url_restaurants, 
        params = params.params, 
        headers = params.headers)
    j_record = json.loads(r.content)
    list_rest = []
    while len(j_record) != 0 :
        r = requests.get(url = url_restaurants, params = params.params, headers = params.headers)
        j_record = json.loads(r.content)
        for obj in j_record:
            list_rest.append(obj)
        params.params['offset'] += 50
    list_rest = json.dumps(list_rest)

    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'restaurants'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        INSERT INTO stg.restaurants (object_value,update_ts) 
        VALUES ('{}', '{}');""".format(list_rest,f_time)
    
    postgres_insert_query_settings = """ 
        INSERT INTO stg.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(f_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

def load_couriers():
    r = requests.get(
        url = url_couriers, 
        params = params.params, 
        headers = params.headers)
    j_record = json.loads(r.content)
    list_cour = []
    while len(j_record) != 0 :
        r = requests.get(url = url_couriers, params = params.params, headers = params.headers)
        j_record = json.loads(r.content)
        for obj in j_record:
            list_cour.append(obj)
        params.params['offset'] += 50
    list_cour = json.dumps(list_cour)

    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'couriers'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        INSERT INTO stg.couriers (object_value,update_ts) 
        VALUES ('{}', '{}');""".format(list_cour,f_time)
    
    postgres_insert_query_settings = """ 
        INSERT INTO stg.settings (workflow_key, workflow_settings) 
        VALUES ('{}','{}');""".format(f_time,current_table)                  
    cur_1.execute(postgres_insert_query)    
    cur_1.execute(postgres_insert_query_settings)  
    
    conn_1.commit()
    conn_1.close()
    cur_1.close()

def load_deliveries():
    r = requests.get(
        url = url_deliveries, 
        params = params.params, 
        headers = params.headers)
    j_record = json.loads(r.content)
    list_del = []
    while len(j_record) != 0 :
        r = requests.get(url = url_deliveries, params = params.params, headers = params.headers)
        j_record = json.loads(r.content)
        for obj in j_record:
            list_del.append(obj)
        params.params['offset'] += 50
    list_del = json.dumps(list_del)

    f_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_table = 'deliveries'
    
    cur_1 = conn_1.cursor()                 
    postgres_insert_query = """ 
        INSERT INTO stg.deliveries (object_value,update_ts) 
        VALUES ('{}', '{}');""".format(list_del,f_time)
    
    postgres_insert_query_settings = """ 
        INSERT INTO stg.settings (workflow_key, workflow_settings) 
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
        'load_stg', 
        default_args=default_args,
        schedule_interval='0/15 * * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['sprint5', 'example'],
) as dag:

    t1 = EmptyOperator(task_id="start")
    with TaskGroup("load_stg_tables") as load_tables:
        t21 = PythonOperator(task_id="restaurants", python_callable=load_restaurants, dag=dag)
        t22 = PythonOperator(task_id="couriers", python_callable=load_couriers, dag=dag)
        t23 = PythonOperator(task_id="deliveries", python_callable=load_deliveries, dag=dag)
    t4 = EmptyOperator(task_id="end")
    
    t1 >> load_tables >> t4