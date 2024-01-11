from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

from utils.alerting.airflow import airflow_callback
EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

default_args = {
    "owner": "dinh.nguyen",
    "start_date": datetime(2021, 9, 7),
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

# schedule_interval = '0 */3 * * *'
schedule_interval = "@daily"

dag = DAG(
    dag_id="sipher_token_transactions",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
    tags=["ethereum", "presentation"],
    catchup=False,
)

sipher_token_transfers = BigQueryExecuteQueryOperator(
    task_id="sipher_token_transfers",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/sipher_token_transfers.sql",
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

token_transaction_sipherians = BigQueryExecuteQueryOperator(
    task_id="token_transaction_sipherians",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/token_transaction_sipherians.sql",
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

transactions_sipherians = BigQueryExecuteQueryOperator(
    task_id="transactions_sipherians",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/sipherians.sql",
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

sipher_token_onwer_by_time = BigQueryExecuteQueryOperator(
    task_id="sipher_token_onwer_by_time",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/sipher_token_owner_by_time.sql",
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

sipher_token_onwer_by_time_all = BigQueryExecuteQueryOperator(
    task_id="sipher_token_onwer_by_time_all",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/sipher_token_owner_by_time_partition.sql",
    params={'bigquery_project': BIGQUERY_PROJECT},
    dag=dag,
)

finance_wallet_transaction = BigQueryExecuteQueryOperator(
    task_id="finance_wallet_transaction",
    use_legacy_sql=False,
    gcp_conn_id="sipher_gcp",
    sql="query/finance_wallet_transaction.sql",
    dag=dag,
)

token_transaction_sipherians >> transactions_sipherians
sipher_token_transfers >> sipher_token_onwer_by_time >> sipher_token_onwer_by_time_all
finance_wallet_transaction
