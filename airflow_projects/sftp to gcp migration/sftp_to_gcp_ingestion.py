from datetime import timedelta, datetime
import json
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Declare variables here
DATASET_NAME = "data_from_sftp"
TABLE_NAME = "clean_data"
FILE_LOCAL_PATH = "source/data/files_corpus"
OBJECT_SRC_1 = "data.csv"
BUCKET_NAME = "data_from_sftp_bucket0"


# Define the default arguments of Composer
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2022, 9, 24),
    'email': ['mattarisf@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# This is the time frequency at which you want to run your jobs
schedule_interval = "@hourly"
    

# Define the DAG (Direct Acyclic Graph) parameters with default_args and schedule_interval
dag = DAG('sftp_to_bq_ingest', default_args=default_args, schedule_interval=schedule_interval)

# Define each tasks for the DAG (each tasks will act as nodes of the graph)
with dag:
	# This task will take the data from the sftp server (source_path) and export it to Google Cloud Storage (destination_bucket)
	copy_file_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="file-copy-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{OBJECT_SRC_1}",
        destination_bucket=BUCKET_NAME,
    )

    # This is just dummy operator to indicate that loading is done from sftp to GCS. It will do nothing.
    loading_finish = DummyOperator(
    	task='loading_finish',
    	dag=dag
    	)

    # This task will import the raw data from GCS to BigQuery Table. You need to specify the source objects in GCS buckets, format and destination table name
    import_to_bq_raw_table = GCSToBigQueryOperator(
		task_id='import_to_bq_raw_table',
	    bucket=BUCKET_NAME,
	    source_objects=["raw_data/data.csv"],
	    source_format="CSV",
	    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
	    autodetect=True,
	    allow_quoted_newlines=False,
	    write_disposition='WRITE_TRUNCATE',
	    dag=dag,
    )

    # This is first row/column quality check, it will validate the query data with the sample data in json file
    with open("include/data.json") as ffv:
        with TaskGroup(group_id="row_quality_checks") as quality_check_group:
            ffv_json = json.load(ffv)
            for id, values in ffv_json.items():
                values["id"] = id
                values["dataset"] = DATASET
                values["table"] = TABLE
                BigQueryCheckOperator(
                    task_id=f"check_row_data_{id}",
                    sql="row_quality_bq_check.sql",
                    use_legacy_sql=False,
                    params=values,
                )

    # Check if the first quality check passed. ShortCircuit OOperator will proceed to next task if pass otherwise it will skip all the downstream tasks. If the first quality check fails, data will not be ingested in BigQuery
    is_row_quality_ok = ShortCircuitOperator(
	    task_id='is_data_quality_ok',
	    provide_context=False,
	    trigger_rule=TriggerRule.ONE_FAILED,
	    dag=dag

    )

    # This is second data quality check where we will count the rows in the BigQuery tables
    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=9,
        use_legacy_sql=False,
    )

    # If the second check is pass then only move the good quality data to BigQuery
    is_bq_row_count_ok = ShortCircuitOperator(
	    task_id='is_data_quality_ok',
	    provide_context=False,
	    trigger_rule=TriggerRule.ONE_FAILED,
	    dag=dag

    )

    # Move the final data to BigQuery
    move_checked_data_to_bq_fin_table = BigQueryInsertJobOperator(
        task_id="move_checked_data_to_bq_fin_table",
        configuration={
            "query": {
                "query": "{% include 'raw_to_fin.sql' %}",
                "useLegacySql": False,
                "allow_large_results": True,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition" "CREATE_IF_NEEDED"
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": DATASET_NAME,
                    "tableId": TABLE_NAME,
                },
            }
        },
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )



    copy_file_from_sftp_to_gcs >> loading_finish >> import_to_bq_raw_table >> quality_check_group >> is_row_quality_ok >> check_bq_row_count >> is_bq_row_count_ok >> move_checked_data_to_bq_fin_table
	