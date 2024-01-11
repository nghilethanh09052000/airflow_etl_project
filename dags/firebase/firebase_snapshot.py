from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

BIGQUERY_PROJECT = Variable.get("bigquery_project")

start_date = set_env_value(production=datetime(2023, 8, 7), dev=datetime(2022, 9, 5))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="@daily", dev="@once")

default_args = {
    "owner": "son.le",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback
}

with DAG(
    dag_id="firebase_snapshot",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:

    playtest_survey = BigQueryExecuteQueryOperator(
        task_id="firebase_snapshot",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/snapshot_firebase.sql",
        dag=dag,
    )
