import os
from datetime import datetime, timedelta
import logging

import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from google.cloud import storage

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

GCP_PROJECT = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("CLOUDSTORAGE_BUCKET_NAME")
BQ_DATASET = os.environ.get("BIGQUERY_DATASET_NAME")
TEMP_DATA_PATH = os.getenv("TEMP_DATA_PATH", "not-found")
URL_PREF = "https://noaa-ghcn-pds.s3.amazonaws.com"


file_names = ["countries", "stations"]
hdrs = {
    "countries": ["code", "name"],
    "stations": [
        "id",
        "latitude",
        "longitude",
        "elevation",
        "state",
        "name",
        "gsn_flag",
        "hcn_crn_flag",
        "wmo_id",
    ],
}
index_column = {
    "countries": [[0, 2], [3, 50]],
    "stations": [
        [0, 11],
        [12, 20],
        [21, 30],
        [31, 37],
        [38, 40],
        [41, 71],
        [72, 75],
        [76, 79],
        [80, 85],
    ],
}
is_string_column = {
    "countries": [True, True],
    "stations": [True, False, False, False, True, True, True, True, True],
}


def txt_to_csv(**kwargs):

    source_file = kwargs["source_file"]
    hdr = kwargs["hdr"]
    index_column = kwargs["index_column"]
    is_string_column = kwargs["is_string_column"]
    with open(source_file) as f_t:
        with open(source_file.replace("txt", "csv"), "w") as f_c:
            f_c.writelines([",".join(hdr) + "\n"])
            for line in f_t:
                columns = []
                for i, index in enumerate(index_column):
                    text = line[index[0] : index[1]].strip()
                    if is_string_column:
                        columns.append('"' + text + '"')
                    else:
                        columns.append(line[index[0] : index[1]])
                f_c.writelines([",".join(columns) + "\n"])


def csv_to_parquet(**kwargs):

    source_file = kwargs["source_file"]
    if not source_file.endswith(".csv"):
        logging.error("File must be in CSV format")
        return
    table = pv.read_csv(source_file)
    pq.write_table(table, source_file.replace(".csv", ".parquet"))


def gcs_upload(bucket, object_name, local_file):
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

    logging.info(BUCKET_NAME)
    logging.info(parquet_gcs_name)
    logging.info(parquet_path)
    logging.info(blob)

    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "end_date": datetime.now() + timedelta(days=1),
    "depends_on_past": False,
    "retries": 2,
}


with DAG(
    dag_id="stations_countries_ingestion",
    description="Download country and station data from S3, convert to parquet, upload to GCS and create tables in BQ",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["climatology"],
) as dag:

    for id in file_names:
        txt_name = f"/ghcnd-{id}.txt"
        data_url = URL_PREF + txt_name
        txt_path = TEMP_DATA_PATH + txt_name
        csv_name = txt_name.replace(".txt", ".csv")
        csv_path = TEMP_DATA_PATH + csv_name
        parquet_name = txt_name.replace(".txt", ".parquet")
        parquet_path = TEMP_DATA_PATH + parquet_name
        parquet_gcs_name = f"{parquet_name[1:]}"
        parquet_gcs_uri = f"gs://{BUCKET_NAME}/{parquet_gcs_name}"
        bq_table_external = f"external_table_{id}"
        table_name = f"{id}"

        download_data = BashOperator(
            task_id=f"download_data_{id}_task",
            bash_command=f"curl -sS {data_url} > {txt_path}",
        )

        txt_to_csv_task = PythonOperator(
            task_id=f"txt_to_csv_{id}_task",
            python_callable=txt_to_csv,
            op_kwargs={
                "source_file": txt_path,
                "hdr": hdrs[id],
                "index_column": index_column[id],
                "is_string_column": is_string_column[id],
            },
        )

        csv_to_parquet_task = PythonOperator(
            task_id=f"csv_to_parquet__{id}_task",
            python_callable=csv_to_parquet,
            op_kwargs={"source_file": csv_path},
        )

        gcs_upload_task = PythonOperator(
            task_id=f"gcs_upload_{id}_task",
            python_callable=gcs_upload,
            op_kwargs={
                "bucket": BUCKET_NAME,
                "object_name": parquet_gcs_name,
                "local_file": parquet_path,
            },
        )

        rm_local_files = BashOperator(
            task_id=f"rm_local_files_{id}_files",
            bash_command=f"rm {csv_path} {parquet_path} {txt_path}",
        )

        bq_external = BigQueryCreateExternalTableOperator(
            task_id=f"bq_external_{id}_task",
            table_resource={
                "tableReference": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": bq_table_external,
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [parquet_gcs_uri],
                },
            },
        )

        CREATE_TABLE_QUERY = f"CREATE OR REPLACE TABLE {GCP_PROJECT}.{BQ_DATASET}.{table_name} \
            AS SELECT * FROM {GCP_PROJECT}.{BQ_DATASET}.{bq_table_external};"

        bq_part = BigQueryInsertJobOperator(
            task_id=f"bq_part_{id}_task",
            configuration={
                "query": {"query": CREATE_TABLE_QUERY, "useLegacySql": False}
            },
        )

    (
        download_data
        >> txt_to_csv_task
        >> csv_to_parquet_task
        >> gcs_upload_task
        >> rm_local_files
        >> bq_external
        >> bq_part
    )
