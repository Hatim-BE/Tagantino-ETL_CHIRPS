from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.extract.chirps_downloader import download_chirps_data
from src.transform.processor import process_file
from src.load.loader import upload_file_to_s3

default_args = {
    'owner': 'A2H',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

import os

LAST_PROCESSED_FILE = '/opt/airflow/data/last_processed_date.txt'

def get_last_processed_date():
    if os.path.exists(LAST_PROCESSED_FILE):
        with open(LAST_PROCESSED_FILE, 'r') as f:
            date_str = f.read().strip()
            return datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime(1981, 1, 1)
    else:
        return datetime(1981, 1, 1) # The beginning

def save_last_processed_date(date_obj):
    with open(LAST_PROCESSED_FILE, 'w') as f:
        f.write(date_obj.strftime("%Y-%m-%d"))

data_type = Variable.get('data_type')
date_format = "%Y-%m-%d" if data_type == "daily" else "%Y-%m"
start_date = get_last_processed_date()
end_date = datetime.strptime(Variable.get("end_date"), date_format) # Or datetime.now()

with DAG(
    'chirps_etl_by_file_grouped',
    default_args=default_args,
    description='ETL by file (sequential extract → transform → load)',
    schedule_interval=None,
    start_date=datetime(2004, 1, 1),
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    @task(task_id=f"ETL_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}")
    def extract_transform_load():
        files = download_chirps_data(
            start_date=start_date,
            end_date=end_date,
            data_type=data_type,
            output_dir=f"/opt/airflow/data/{data_type}/raw",
            indefinite_mode=False,
        )
        for file_path in files:

            transformed_file = process_file(
                file_path=file_path,
                decompress=True,
                clip=True,
                convert_to_csv=True
            )

            upload_file_to_s3(
                file_path=transformed_file,
                bucket=None,
                key=None
            )
    @task(task_id="save_last_processed_date")
    def update_last_date():
        save_last_processed_date(end_date)

    start >> extract_transform_load() >> update_last_date() >> end
