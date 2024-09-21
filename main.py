from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract import fetch_and_save_data
from datetime import datetime
from load import load_data, upload_data_gsheets
from transform import transform_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 31),
    'retries': 1,
}

dag = DAG(
    'api_data_dag',
    default_args=default_args,
    description='Fetch API data and save as JSON',
    schedule_interval='@once',
)

fetch_data_task = PythonOperator(
    task_id='fetch_and_save_data',
    python_callable=fetch_and_save_data,
    provide_context=True,
    dag=dag,
)
upload_data_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

upload_data_to_gsheets_task = PythonOperator(
    task_id='upload_data_to_gsheets',
    python_callable=upload_data_gsheets,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> upload_data_task >> transform_data_task >> upload_data_to_gsheets_task