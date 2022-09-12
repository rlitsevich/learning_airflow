from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize
from datetime import datetime


def _process_rate(ti):
    rate = ti.xcom_pull(task_ids="extract_rate")
    processed_rate = json_normalize(
        {'updated': rate['time']['updatedISO'],
         'usd_rate': rate['bpi']['USD']['rate_float'],
         'gbp_rate': rate['bpi']['GBP']['rate_float'],
         'eur_rate': rate['bpi']['EUR']['rate_float']})
    processed_rate.to_csv('/tmp/processed_rate.csv', index=None, header=False)


def _store_rate():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(sql="COPY btc_rates FROM stdin WITH DELIMITER ',' ",
                     filename='/tmp/processed_rate.csv')


with DAG('btc_rates_processing', start_date=datetime(2022, 1, 1), schedule_interval='@hourly', catchup=False) as dag:
    create_table = PostgresOperator(
        postgres_conn_id='postgres',
        task_id='create_table',
        sql='''
        CREATE TABLE IF NOT EXISTS btc_rates(
            updated TIMESTAMP NOT NULL,
            usd_rate NUMERIC NOT NULL, 
            gbp_rate NUMERIC NOT NULL, 
            eur_rate NUMERIC NOT NULL);''')

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='coindesk',
        endpoint='v1/bpi/currentprice.json'
    )

    extract_rate = SimpleHttpOperator(
        task_id='extract_rate',
        http_conn_id='coindesk',
        endpoint='v1/bpi/currentprice.json',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_rate = PythonOperator(
        task_id='process_rate',
        python_callable=_process_rate
    )

    store_rate = PythonOperator(
        task_id='store_rate',
        python_callable=_store_rate
    )

    create_table >> is_api_available >> extract_rate >> process_rate >> store_rate
