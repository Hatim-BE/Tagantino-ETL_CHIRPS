from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.extract.chirps_downloader import download_chirps_data
from src.transform.processor import process_file
from utils import chirps_file_exists

default_args = {
    'owner': 'A2H',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

with DAG(
    'chirps_etl_by_file_grouped',
    default_args=default_args,
    description='ETL by file (sequential extract → transform)',
    schedule_interval=None,
    start_date=datetime(2004, 1, 1),
    catchup=False,
) as dag:
    
    def make_extract_task(single_date, date_str):
        @task(task_id=f"extract_{date_str}")
        def extract():
            files = download_chirps_data(
                start_date=single_date,
                end_date=single_date,
                data_type='daily',
                output_dir='/opt/airflow/data/raw',
                indefinite_mode=False,
            )
            return files[0] if files else None  # single file
        return extract()

    def make_transform_task(date_str):
        @task(task_id=f"transform_{date_str}")
        def transform(file_path: str):
            return process_file(
                file_path=file_path,
                decompress=True,
                clip=True,
                convert_to_csv=True
            )
        return transform


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start_date = datetime(2023, 12, 31)
    end_date = datetime(2024, 1, 1)

    for single_date in daterange(start_date, end_date):
        date_str = single_date.strftime('%Y%m%d')
        if not chirps_file_exists(single_date):
            continue

        with TaskGroup(group_id=f'etl_{date_str}') as etl_group:
            extract_task = make_extract_task(single_date, date_str)
            transform_task = make_transform_task(date_str)
            transform_task(extract_task)

        start >> etl_group >> end
