from datetime import datetime, timedelta
from airflow import DAG
from utils.common import set_env_value
from utils.alerting.airflow import airflow_callback
from google_play.operators.google_play_reports import GCSFileTransformOperator
from airflow.models import Variable

DAG_START_DATE = set_env_value(
    production=datetime(2023, 12, 12), dev=datetime(2023, 12, 12)
)
DAG_END_DATE = set_env_value(production=None, dev=None)
DAG_SCHEDULE_INTERVAL = set_env_value(production="@daily", dev="@once")
GCP_CONN_ID = "sipher_gcp"
DESTINATION_BUCKET = Variable.get("ingestion_gcs_bucket", "atherlabs-test")
DESTINATION_OBJECT = "google_play"
SOURCE_BUCKET = "pubsite_prod_7990528982458715897"

default_args = {
    "owner": "vy.pham",
    "start_date": DAG_START_DATE,
    "end_date": DAG_END_DATE,
    "trigger_rule": "all_done",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_callback,
}

with DAG(
    dag_id="google_play_iap",
    default_args=default_args,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    tags=["google_play", "raw_reports"],
    catchup=True
) as dag:
    
    financial_reports__earnings_reports = GCSFileTransformOperator(
        task_id= "financial_reports__earnings_reports",
        gcp_conn_id=GCP_CONN_ID,
        source_bucket= SOURCE_BUCKET,
        source_object = "earnings",
        destination_bucket= DESTINATION_BUCKET,
        destination_object= f"{DESTINATION_OBJECT}/financial__earnings_reports",
        ds= "{{ macros.ds_format(macros.ds_add(ds, -30), '%Y-%m-%d', '%Y%m') }}",
    )

    financial_reports__estimated_sales_reports = GCSFileTransformOperator(
        task_id= "financial_reports__estimated_sales_reports",
        gcp_conn_id=GCP_CONN_ID,
        source_bucket= SOURCE_BUCKET,
        source_object = "sales",
        destination_bucket= DESTINATION_BUCKET,
        destination_object= f"{DESTINATION_OBJECT}/financial__estimated_sales_reports",
        ds= "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}",
    )

    statistics_reports__installs_reports = GCSFileTransformOperator(
        task_id= "statistics_reports__installs_reports",
        gcp_conn_id=GCP_CONN_ID,
        source_bucket= SOURCE_BUCKET,
        source_object = "stats/installs",
        destination_bucket= DESTINATION_BUCKET,
        destination_object= f"{DESTINATION_OBJECT}/statistics__installs_reports",
        ds= "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}",
    )
