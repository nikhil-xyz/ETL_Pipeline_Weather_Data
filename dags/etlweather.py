from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

LATITUDE = '51.5071'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# DAG
with DAG(dag_id = 'weather_etl_pipeline',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dags:
    @task()
    def extract_weather_data():
        """
        This task retrieves weather data from the Open Meteo API.
        """
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f'Failed to retrieve weather data: {response.status_code}')

    
    @task()
    def transform_weather_data(weather_data):
        """
        This task transforms the retrieved weather data.
        """
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude' : LATITUDE,
            'longitude' : LONGITUDE,
            'temperature': current_weather['temperature'],
            'winddirection': current_weather['winddirection'],
            'windspeed': current_weather['windspeed'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data


    @task()
    def load_weather_data(transformed_data):
        """
        This task loads the transformed weather data into a PostgreSQL database.
        """
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            winddirection FLOAT,
            windspeed FLOAT,
            weathercode INTEGER
            );
            """)

        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, winddirection, windspeed, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (transformed_data['latitude'], 
            transformed_data['longitude'], 
            transformed_data['temperature'], 
            transformed_data['winddirection'], 
            transformed_data['windspeed'], 
            transformed_data['weathercode'])
        )

        conn.commit()
        cursor.close()
        conn.close()


    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)