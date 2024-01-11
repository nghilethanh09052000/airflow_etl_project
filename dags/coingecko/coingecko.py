from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from coingecko.scripts.coingecko_coin_currency import EtherScanCurrency
from coingecko.scripts.coingecko_coin_list_unnest import EtherScan
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

default_args = {
    "owner": "tri.nguyen",
    "start_date": datetime(2022, 5, 4),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

schedule_interval = "0 2,6 * * *"

dag = DAG(
    "coingecko",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=2,
    tags=["coingecko", "raw"],
    catchup=False,
)

etherscan = EtherScan()
etherscancurrency = EtherScanCurrency()

coingecko_coin_list_unnest = PythonOperator(
    task_id="coingecko_coin_list_unnest",
    dag=dag,
    provide_context=True,
    python_callable=etherscan.run,
)

coingecko_coin_currency = PythonOperator(
    task_id="coingecko_coin_currency",
    dag=dag,
    provide_context=True,
    python_callable=etherscancurrency.run,
)

coingecko_coin_currency_all = BigQueryExecuteQueryOperator(
    task_id="coingecko_coin_currency_all",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/coingecko_coin_currency.sql",
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

coingecko_coin_list_unnest >> coingecko_coin_currency >> coingecko_coin_currency_all
