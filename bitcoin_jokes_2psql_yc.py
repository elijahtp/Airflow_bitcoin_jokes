import logging as _log
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

import pydantic
import requests
import json
 
from typing import Optional
from typing import List
from pydantic import BaseModel, ValidationError

PG_CONN_ID = 'postgres_yc_managed_instance'
PG_TABLE = 'bitcoin_jokes'
url_response_bitcoin = requests.get('https://api.coincap.io/v2/rates')
url_response_chucknorris = requests.get('https://api.chucknorris.io/jokes/random') 


def conn_bitcoin():
    try:
        url_response_bitcoin.raise_for_status()
    except Exception as ex:
        print(ex)
    _log.info('connecting to bitcoin_rate api')

def conn_chucknorris():   
    try:
        url_response_chucknorris.raise_for_status()
    except Exception as ex:
        print(ex)
    _log.info('connecting to chucknorris api')


def get_bitcoin_rate():

    dict_of_values_bitcoin = json.dumps(url_response_bitcoin.json())
    class Data_bitcoin(BaseModel):
        id: str
        symbol: str
        currencySymbol: Optional[str]
        rateUsd: str
    class JSON_bitcoin(BaseModel):
        data: List[Data_bitcoin]
        timestamp: int
    try:
        json_bitcoin = JSON_bitcoin.parse_raw(dict_of_values_bitcoin)
    except ValidationError as e:
        print("Exception",e.json())
    else: 
        for i in range(len(json_bitcoin.data)):
            if json_bitcoin.data[i].id == "bitcoin":           
                bitcoin_rate = json_bitcoin.data[i].rateUsd
                timestamp_msr = json_bitcoin.timestamp  
    return bitcoin_rate, timestamp_msr
    _log.info('getting bitcoin_rate value')                   

def get_chucknorris_joke():

    dict_of_values_chucknorris = json.dumps(url_response_chucknorris.json())
    
    class ChucknorrisJoke(BaseModel):
        value: str        
    try:
        chucknorrisjoke = ChucknorrisJoke.parse_raw(dict_of_values_chucknorris)
    except ValidationError as e:
        print("Exception",e.json())
    else:         
        chucknorris_joke = chucknorrisjoke.value
    return chucknorris_joke     
    _log.info('getting Chuck Norris joke')                   
    

def put_to_psql():
    population_string = """ CREATE TABLE IF NOT EXISTS {0} (BitcoinRate INT, ChuckNorrisJoke TEXT, Timestamp BIGINT, CurrentTime timestamp);
                            INSERT INTO {0} 
                            (BitcoinRate, ChuckNorrisJoke, Timestamp, CurrentTime) 
                            VALUES ({1},$uniq_tAg${2}$uniq_tAg$,{3},current_timestamp);
                        """ \
                        .format(PG_TABLE, get_bitcoin_rate()[0], get_chucknorris_joke(), get_bitcoin_rate()[1])
    return population_string
    _log.info('table created and populated successfully')

args = {
    'owner': 'airflow',
    'catchup': 'False'
}

with DAG(
    dag_id='bitcoin_jokes_2psql_yc',
    default_args=args,
    schedule_interval='*/30 * * * *',
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['4otus', 'API'],
    catchup=False,
) as dag:
    task_connect_bitcoin = PythonOperator(
        task_id='connect_to_api_bitcoin', 
        python_callable=conn_bitcoin, 
        dag=dag
    )

    task_connect_chucknorris = PythonOperator(
        task_id='connect_to_api_chucknorris', 
        python_callable=conn_chucknorris, 
        dag=dag
    )

    task_get_bitcoin_rate = PythonOperator(
        task_id='get_bitcoin_rate', 
        python_callable=get_bitcoin_rate, 
        dag=dag
    )

    task_get_chucknorris_joke = PythonOperator(
        task_id='get_chucknorris_joke', 
        python_callable=get_chucknorris_joke, 
        dag=dag
    )

    task_populate = PostgresOperator(
        task_id="put_to_psql_bitcoin_rate_and_chucknorris_joke",
        postgres_conn_id=PG_CONN_ID,
        sql=put_to_psql(),
        dag=dag
    )


task_connect_bitcoin >> task_connect_chucknorris >> task_get_bitcoin_rate >> task_get_chucknorris_joke >> task_populate
