from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2

default_args = {
    'owner': 'santiago',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def on_failure_callback(context):
    #EXTRAER INFORMACIÓN DE ERROR
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date =  context['execution_date']
    exception = context.get('exception')
    #MENSAJE DE ERROR
    print(f'Fallo en pipeline {dag_id}')
    print(f'Tarea {task_id}')       
    print(f'En ejecución {execution_date}')       
    print(f'Error: {exception}')
    print(f'Fecha {execution_date}')   

def extract(**context):
    #Extracción de datos del API
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 4.6097,
        "longitude": -74.0817,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "America/Bogota",
        "past_days": 7
    }
    #Realización de la petición con timeout
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    #Validación de datos
    if not data.get('daily') or not data['daily'].get('time'):
        raise ValueError("Datos diarios no encontrados en la respuesta del API")

    #Guardar los datos
    context['ti'].xcom_push(key='raw_data', value=data)
    print(f"✓ Extraídos {len(data['daily']['time'])} días")

def transform(**context):
    #Transformación de los datos
    raw = context['ti'].xcom_pull(key='raw_data', task_ids='extract_task')
    df = pd.DataFrame({
        'fecha': raw['daily']['time'],
        'temp_max': raw['daily']['temperature_2m_max'],
        'temp_min': raw['daily']['temperature_2m_min'],
        'precipitacion_mm': raw['daily']['precipitation_sum'],
    })
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['temp_promedio'] = (df['temp_max'] + df['temp_min']) / 2
    df.dropna(inplace=True)

    #Verificacion de dataframe =! vacio
    if df.empty:
        raise ValueError("El DataFrame de transformación está vacío.")

    df['fecha'] = df['fecha'].astype(str) #Cambio de formato porque JSON solo maneja numeros o strings.
    context['ti'].xcom_push(key='clean_data', value=df.to_dict('records'))
    print(f"✓ Transformadas {len(df)} filas")

def load(**context):
    #Carga de los datos a Postgres
    records = context['ti'].xcom_pull(key='clean_data', task_ids='transform_task')
    
    #Validación de registros
    if not records:
        raise ValueError("No hay registros para cargar en la base de datos.")
   
    conn = psycopg2.connect(
        host="postgres", database="clima_db",
        user="airflow", password="airflow"
    )
    
    # Creación de la tabla y carga de datos
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clima_bogota (
            fecha DATE PRIMARY KEY,
            temp_max FLOAT,
            temp_min FLOAT,
            temp_promedio FLOAT,
            precipitacion_mm FLOAT,
            ingested_at TIMESTAMP DEFAULT NOW()
        )
    """)
    for r in records:
        cur.execute("""
            INSERT INTO clima_bogota
                (fecha, temp_max, temp_min, temp_promedio, precipitacion_mm)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (fecha) DO UPDATE SET
                temp_max = EXCLUDED.temp_max,
                temp_min = EXCLUDED.temp_min,
                temp_promedio = EXCLUDED.temp_promedio,
                precipitacion_mm = EXCLUDED.precipitacion_mm
        """, (r['fecha'], r['temp_max'], r['temp_min'],
              r['temp_promedio'], r['precipitacion_mm']))
    conn.commit()
    cur.close()
    conn.close()
    print(f"✓ Cargados {len(records)} registros en Postgres")

with DAG(
    #El DAF es modelo visual y lógico utilizado para orquestar,
    #programar y estructurar flujos de trabajo de datos. Como ETL y ML.

    #Definición del DAG
    dag_id='clima_bogota_etl',
    default_args=default_args,
    description='Pipeline ETL clima Bogotá - Portafolio',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['clima', 'etl', 'portafolio'],
) as dag:
    #Definición de las tareas
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    extract_task >> transform_task >> load_task