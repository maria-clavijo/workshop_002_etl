from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys 
import os
sys.path.append(os.path.abspath("/opt/airflow/dags/Airflow/"))

from transf_grammys import extract_db, transformation_db
from transf_spotify import extract_csv, transformation_csv
from merge_data import merge, load_to_db


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'workshop2_dags',
    default_args=default_args,
    description='Dag for an ETL process: Workshop 2',
    schedule_interval='@daily',
) as dag:

    merge_data = PythonOperator(
        task_id = 'merge',
        python_callable = merge,
        provide_context = True,
    )

    extract_db_data = PythonOperator(
        task_id = 'extract_db',
        python_callable = extract_db,
        provide_context = True,
    )

    transform_db = PythonOperator(
        task_id = 'transformation_db',
        python_callable = transformation_db,
        provide_context = True,
    )

    extract_csv_data = PythonOperator(
        task_id = 'extract_csv',
        python_callable = extract_csv,
        provide_context = True,
    )

    transform_csv = PythonOperator(
        task_id = 'transformation_csv',
        python_callable = transformation_csv,
        provide_context = True,
    )


    load_data_db = PythonOperator(
        task_id ='load_to_db',
        python_callable = load_to_db,
        provide_context = True,
    )

    extract_db_data >> transform_db >> merge_data
    extract_csv_data >> transform_csv >> merge_data
    merge_data >> load_data_db
    