from calendar import month
import logging
import os
from datetime import datetime, timedelta
import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.dummy_operator import DummyOperator


BUCKET = "dtc_data_lake_original-brace-339113"
PROJECT_ID = "original-brace-339113"


# dataset_file = "fhv_tripdata_{month_date}.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# path_to_creds = f"{path_to_local_home}/google_credentials.json"

path_to_creds = f"google_credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_creds

DATASET_NAME = "trips_data_all"
TABLE_NAME = "fhv_trips"
START_DATE = datetime(2019, 1, 1)
END_DATE = datetime(2019, 12, 30)
TIME_DELTA_STEP = timedelta(weeks=4)

def gen_dates(start, end, step):
    result = set()
    while start <= end:
        result.add(start.strftime('%Y-%m'))
        start += step
    return list(result)

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("File is not CSV")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', 'parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(path_to_creds)
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(60),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    dag_id="fhv_data_ingestion_gcs_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['example'],
)

dates = gen_dates(start=START_DATE, end=END_DATE, step=TIME_DELTA_STEP)

start_task = DummyOperator(
    task_id="start",
    dag=dag)

end_task = DummyOperator(
    task_id="end",
    dag=dag)

for date in dates:

    dataset_file = f"fhv_tripdata_{date}.csv"
    dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"

    download_dataset_task = BashOperator(
        dag=dag,
        task_id=f"download_dataset_task_{date}",
        bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        dag=dag,
        task_id=f'format_to_parquet_task_{date}',
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}"
        }
    )

    local_to_gcs_task = PythonOperator(
        dag=dag,
        task_id=f"local_to_gcs_task_{date}",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",

        },
    )

    gcs_to_bq_task = GCSToBigQueryOperator(
        dag=dag,
        task_id=f'gcs_to_bq_task_{date}',
        bucket=BUCKET,
        source_objects=[f"raw/{dataset_file}"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        # schema_fields=[
        #     {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
        # ],
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1
    )


    start_task >> download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> gcs_to_bq_task >> end_task