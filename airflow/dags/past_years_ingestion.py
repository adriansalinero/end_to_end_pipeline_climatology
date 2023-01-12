import os
from datetime import datetime
import logging

import pyarrow.csv as pv
import pyarrow.compute as pc
import pyarrow.parquet as pq

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from google.cloud import storage

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)


GCP_PROJECT = os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("CLOUDSTORAGE_BUCKET_NAME")
BQ_DATASET = os.environ.get("BIGQUERY_DATASET_NAME")
TEMP_DATA_PATH = os.getenv("TEMP_DATA_PATH", "not-found")
STARTING_YEAR = int(os.getenv("STARTING_YEAR", "2022"))

URL_PREF = "https://noaa-ghcn-pds.s3.amazonaws.com/csv/by_year"
schema = {
    "id": "string",
    "date": "string",
    "element": "string",
    "data_value": "int32",
    "m_flag": "string",
    "q_flag": "string",
    "s_flag": "string",
    "obs_time": "int32",
}


def csv_to_parquet(**kwargs):

    source_file = kwargs["source_file"]
    header = kwargs["columns"]
    csv_schema = kwargs["csv_schema"]
    if not source_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(
        source_file,
        read_options=pv.ReadOptions(column_names=header, skip_rows=1),
        convert_options=pv.ConvertOptions(column_types=csv_schema),
    )
    table = (
        table.append_column(
            "casted_date",
            pc.strptime(table.column("date"), format="%Y%m%d", unit="s").cast("date32"),
        )
        .drop(["date"])
        .rename_columns(
            [
                "id",
                "meteorological_element",
                "data_value",
                "flag_m",
                "flag_q",
                "flag_s",
                "observation_time",
                "date",
            ]
        )
    )
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
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(STARTING_YEAR, 1, 1),
    "end_date": datetime.now(),
    "depends_on_past": False,
    "retries": 2,
}

with DAG(
    dag_id="past_years_ingestion",
    description="Download past year data from S3, convert to parquet, upload to GCS and create tables in BQ",
    schedule_interval="0 0 2 1 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["climatology"],
) as dag:

    year = '{{dag_run.logical_date.strftime("%Y")}}'
    columns = [
        "id",
        "date",
        "element",
        "data_value",
        "m_flag",
        "q_flag",
        "s_flag",
        "obs_time",
    ]
    csv_name = f"/{year}.csv"
    data_url = URL_PREF + csv_name
    csv_path = TEMP_DATA_PATH + csv_name
    parquet_name = csv_name.replace(".csv", ".parquet")
    parquet_path = TEMP_DATA_PATH + parquet_name
    parquet_gcs_name = f"{parquet_name[1:]}"
    parquet_gcs_uri = f"gs://{BUCKET_NAME}/{parquet_gcs_name}"
    bq_table_external = f"external_table_{year}"
    table_name = f"{year}"

    download_data = BashOperator(
        task_id="download_data", bash_command=f"curl -sS {data_url} > {csv_path}"
    )

    ge_raw_checkpoint = GreatExpectationsOperator(
        task_id="ge_raw_checkpoint",
        data_context_root_dir="./great_expectations",
        checkpoint_name="getting_started_checkpoint",
        return_json_dict=True,
        checkpoint_kwargs={
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": f"{year}.csv",
                        "data_connector_query": {"index": -1},
                    },
                    "expectation_suite_name": "climatology_year.demo",
                },
            ]
        },
    )

    csv_to_parquet_task = PythonOperator(
        task_id="csv_to_parquet_task",
        python_callable=csv_to_parquet,
        op_kwargs={"source_file": csv_path, "columns": columns, "csv_schema": schema},
    )

    gcs_upload_task = PythonOperator(
        task_id="gcs_upload_task",
        python_callable=gcs_upload,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": parquet_gcs_name,
            "local_file": parquet_path,
        },
    )

    rm_local_files = BashOperator(
        task_id="rm_local_files", bash_command=f"rm {csv_path} {parquet_path}"
    )

    bq_external = BigQueryCreateExternalTableOperator(
        task_id="bq_external",
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
        PARTITION BY date \
        CLUSTER BY id AS \
        SELECT * FROM {GCP_PROJECT}.{BQ_DATASET}.{bq_table_external};"

    bq_part = BigQueryInsertJobOperator(
        task_id="bq_part",
        configuration={"query": {"query": CREATE_TABLE_QUERY, "useLegacySql": False}},
    )

    (
        download_data
        >> ge_raw_checkpoint
        >> csv_to_parquet_task
        >> gcs_upload_task
        >> rm_local_files
        >> bq_external
        >> bq_part
    )
