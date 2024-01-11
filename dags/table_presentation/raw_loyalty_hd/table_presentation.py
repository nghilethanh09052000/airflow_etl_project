from datetime import datetime
from airflow.models import Variable

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator

from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

start_date = set_env_value(production=datetime(2022, 11, 3), dev=datetime(2022, 11, 3))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="0 5 * * *", dev="@once")

default_args = {
    "owner": "hoang.dang",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

dag = DAG(
    dag_id="table_presentation",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    tags=["presentation"],
)


raw_loyalty_hd = BigQueryExecuteQueryOperator(
    task_id="raw_loyalty_hd",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/raw_loyalty_hd.sql",
    dag=dag,
    params={"bigquery_project": BIGQUERY_PROJECT},
)

quest_dashboard_hd = BigQueryExecuteQueryOperator(
    task_id="quest_dashboard_hd",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/quest_dashboard_hd.sql",
    dag=dag,
    params={"bigquery_project": BIGQUERY_PROJECT},
)

raw_loyalty_hd
quest_dashboard_hd
