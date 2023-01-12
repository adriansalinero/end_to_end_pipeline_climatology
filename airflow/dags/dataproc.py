import os
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
    DataprocCreateClusterOperator,
)

from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator


GCP_PROJECT = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("CLOUDSTORAGE_BUCKET_NAME")
BQ_DATASET = os.environ.get("BIGQUERY_DATASET_NAME")
GCP_REGION = os.environ.get("GCP_REGION")
STARTING_YEAR = os.getenv("STARTING_YEAR", "2022")
CLUSTER = os.getenv("GCP_DATAPROC_CLUSTER", "climatology")
TEMP_DATA_PATH = os.getenv("TEMP_DATA_PATH", "not-found")
SOURCE_PATH = os.getenv("SOURCE_PATH", "not-found")
source_name = "/transform_year_tables.py"
source_file_path = SOURCE_PATH + source_name
gcs_file_path = f"sources{source_name}"
gcs_uri = f"gs://{BUCKET_NAME}/{gcs_file_path}"

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=2),
    "depends_on_past": False,
    "retries": 2,
}


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
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


with DAG(
    dag_id="dataproc",
    description="Transform parquet files stored in GCS and create tables in BQ",
    schedule_interval="0 6 * * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["climatology"],
) as dag:

    ENDING_YEAR = '{{dag_run.logical_date.strftime("%Y")}}'

    dataproc_create_cluster = DataprocCreateClusterOperator(
        task_id="dataproc_create_cluster",
        project_id=GCP_PROJECT,
        cluster_name=CLUSTER,
        num_workers=2,
        worker_machine_type="n2-standard-4",
        region=GCP_REGION,
    )

    upload_py_to_gcs_task = PythonOperator(
        task_id="upload_py_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": gcs_file_path,
            "local_file": source_file_path,
        },
    )

    dataproc_job_submit = DataprocSubmitPySparkJobOperator(
        task_id="dataproc_job_submit",
        main=gcs_uri,
        arguments=[STARTING_YEAR, f"{ENDING_YEAR}"],
        region=GCP_REGION,
        cluster_name=CLUSTER,
        dataproc_jars=[
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"
        ],
    )

    dataproc_delete_cluster = DataprocDeleteClusterOperator(
        task_id="dataproc_delete_cluster",
        project_id=GCP_PROJECT,
        cluster_name=CLUSTER,
        region=GCP_REGION,
        trigger_rule="all_done",
    )

    (
        dataproc_create_cluster
        >> upload_py_to_gcs_task
        >> dataproc_job_submit
        >> dataproc_delete_cluster
    )
