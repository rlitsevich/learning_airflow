from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import pandas as pd
from pandas import json_normalize
from datetime import datetime, timedelta


def _process_dates(dag_run):
    params = {'begin_date': int(datetime.timestamp(dag_run.execution_date.replace(hour=0, minute=0, second=0))),
              'end_date': int(datetime.timestamp(dag_run.execution_date.replace(hour=23, minute=59, second=59)))}


def _process_arrivals(ti, dag_run):
    arrivals = ti.xcom_pull(task_ids="extract_arrival")
    df = pd.DataFrame(arrivals)
    df['rep_date'] = dag_run.execution_date.date()
    df.to_csv('/tmp/processed_arrivals.csv', index=None, header=False)


def _store_arrivals():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(sql="COPY arrivals FROM stdin WITH DELIMITER ',' NULL ''",
                     filename='/tmp/processed_arrivals.csv')


def _aggregate_arrivals():
    hook = PostgresHook(postgres_conn_id='postgres')
    df = hook.get_pandas_df(sql='select * from arrivals ')

    ddf = df.groupby('rep_date')[['icao24']].count()
    ddf.to_csv('/tmp/aggregated_arrivals.csv', index=None, header=False)


def _store_aggregated_arrivals():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(sql="COPY rep_arrivals FROM stdin WITH DELIMITER ',' ",
                     filename='/tmp/aggregated_arrivals.csv')


with DAG('arrivals_processing', start_date=datetime(2022, 8, 1), schedule_interval='@daily', catchup=True,
         params={
             "begin_date": int(datetime.timestamp(datetime(2022, 8, 1, 0, 0, 0))),
             "end_date": int(datetime.timestamp(datetime(2022, 8, 1, 23, 59, 59)))
         }) as dag:
    create_data_table = PostgresOperator(
        postgres_conn_id='postgres',
        task_id='create_data_table',
        sql='''
        CREATE TABLE IF NOT EXISTS arrivals(
            icao24 varchar, 
            first_seen integer, 
            est_departure_airport varchar, 
            last_seen integer , 
            est_arrival_airport varchar, 
            call_sign varchar,
            estDepartureAirportHorizDistance float4,
            estDepartureAirportVertDistance float4,
            estArrivalAirportHorizDistance float4,
            estArrivalAirportVertDistance float4,
            departureAirportCandidatesCount integer,
            arrivalAirportCandidatesCount integer,
            rep_date DATE NOT NULL);'''
    )

    create_agg_table = PostgresOperator(
        postgres_conn_id='postgres',
        task_id='create_agg_table',
        sql='''
        CREATE TABLE IF NOT EXISTS rep_arrivals(
            rep_date date NOT NULL, 
            arr_count integer NOT NULL);'''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='opensky_api',
        endpoint='api/states/all'
    )

    extract_arrivals = SimpleHttpOperator(
        task_id='extract_arrivals',
        http_conn_id='opensky_api',
        endpoint='api/flights/arrival',
        method='GET',
        data={'airport': 'UUDD',
              'begin': '{{ params.begin_date }}',
              'end': '{{ params.end_date }}'},
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_arrivals = PythonOperator(
        task_id='process_arrivals',
        python_callable=_process_arrivals
    )

    store_arrivals = PythonOperator(
        task_id='store_arrivals',
        python_callable=_store_arrivals
    )

    process_dates = PythonOperator(
        task_id='process_dates',
        python_callable=_process_dates
    )

    truncate_agg_table = PostgresOperator(
        task_id='truncate_agg_table',
        postgres_conn_id='postgres',
        sql="TRUNCATE TABLE rep_arrivals"
    )

    aggregate_arrivals = PythonOperator(
        task_id='aggregate_arrivals',
        python_callable=_aggregate_arrivals
    )

    store_agg_arrivals = PythonOperator(
        task_id='store_agg_arrivals',
        python_callable=_store_aggregated_arrivals
    )

    create_data_table >> create_agg_table >> is_api_available >> process_dates
    process_dates >> extract_arrivals >> process_arrivals >> store_arrivals >> truncate_agg_table
    truncate_agg_table >> aggregate_arrivals >> store_agg_arrivals
