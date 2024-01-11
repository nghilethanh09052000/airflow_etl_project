from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from data_quality_control.scripts.data_quality_score import DataQualityScore
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

default_args = {
    "owner": "hoang.dang",
    "start_date": datetime(2023, 3, 13),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

schedule_interval = set_env_value(production="0 1 * * *", dev="@once")

dag = DAG(
    "data_quality_control",
    default_args=default_args,
    schedule_interval=schedule_interval,
    tags=["data_quality_control", "data_quality_score"],
    catchup=False,
)

data_quality_score = PythonOperator(
    task_id="data_quality_score",
    dag=dag,
    python_callable=DataQualityScore().run,
)

data_quality_score
