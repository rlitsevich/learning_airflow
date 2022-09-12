from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize
from datetime import datetime


def _process_activity(ti):
    activity = ti.xcom_pull(task_ids="extract_activity")
    # activity = activity['return_value'][0]
    processed_activity = json_normalize(
        {'act_description': activity['activity'],
         'act_key': activity['key'],
         'act_type': activity['type'],
         'act_participants': activity['participants'],
         'act_price': activity['price'],
         'act_link': activity['link'],
         'act_accessibility': activity['accessibility']})
    processed_activity.to_csv('/tmp/processed_activity.csv', index=None, header=False)


def _store_activity():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(sql="COPY activities FROM stdin WITH DELIMITER ',' ",
                     filename='/tmp/processed_activity.csv')


with DAG('activities_processing', start_date=datetime(2022, 1, 1), schedule_interval='@hourly', catchup=False) as dag:
    create_table = PostgresOperator(
        postgres_conn_id='postgres',
        task_id='create_table',
        sql='''
        CREATE TABLE IF NOT EXISTS activities(
            act_description TEXT NOT NULL,
            act_key TEXT NOT NULL, 
            act_type TEXT NOT NULL, 
            act_participants INTEGER NOT NULL, 
            act_price NUMERIC NOT NULL, 
            act_link TEXT NOT NULL, 
            act_accessibility NUMERIC NOT NULL);''')

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='activity_api',
        endpoint='api/activity/'
    )

    extract_activity = SimpleHttpOperator(
        task_id='extract_activity',
        http_conn_id='activity_api',
        endpoint='api/activity/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_activity = PythonOperator(
        task_id='process_activity',
        python_callable=_process_activity
    )

    store_activity = PythonOperator(
        task_id='store_activity',
        python_callable=_store_activity
    )

    create_table >> is_api_available >> extract_activity >> process_activity >> store_activity
