from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator

from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value

start_date = set_env_value(production=datetime(2022, 11, 3), dev=datetime(2022, 11, 3))
end_date = set_env_value(production=None, dev=None)
schedule_interval = set_env_value(production="@daily", dev="@once")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

default_args = {
    "owner": "tri.nguyen",
    "start_date": start_date,
    "end_date": end_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback
}

with DAG(
    dag_id="staking_pool_data",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=True,
    tags=["staking", "public_data"],
) as dag:

    staking_pool_transaction = BigQueryExecuteQueryOperator(
        task_id="staking_pool_transaction",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/staking_pool_transaction.sql",
        dag=dag,
    )

    staking_pool_transaction_agg = BigQueryExecuteQueryOperator(
        task_id="staking_pool_transaction_agg",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/staking_pool_transaction_agg.sql",
        dag=dag,
    )

    sipher_staking_claimed_rewards = BigQueryExecuteQueryOperator(
        task_id="sipher_staking_claimed_rewards",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/sipher_staking_claimed_rewards.sql",
        params={'bigquery_project': BIGQUERY_PROJECT},
        dag=dag,
    )

    sipher_staking_distributed = BigQueryExecuteQueryOperator(
        task_id="sipher_staking_distributed",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/sipher_staking_distributed.sql",
        params={'bigquery_project': BIGQUERY_PROJECT},
        dag=dag,
    )

    sipher_staking_rewards_claimed_and_withdrawn = BigQueryExecuteQueryOperator(
        task_id="sipher_staking_rewards_claimed_and_withdrawn",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/sipher_staking_rewards_claimed_and_withdrawn.sql",
        params={'bigquery_project': BIGQUERY_PROJECT},
        dag=dag,
    )

    # fct_staking_pool_transaction =  DbtRunOperator(
    #     task_id="fct_staking_pool_transaction", 
    #     models="fct_staking_pool_transaction", 
    #     dag=dag
    # )

    # agg_staking_pool_transaction =  DbtRunOperator(
    #     task_id="agg_staking_pool_transaction", 
    #     models="agg_staking_pool_transaction", 
    #     dag=dag
    # )

    # fct_sipher_staking_claimed_rewards =  DbtRunOperator(
    #     task_id="fct_sipher_staking_claimed_rewards", 
    #     models="fct_sipher_staking_claimed_rewards", 
    #     dag=dag
    # )

    # fct_sipher_staking_distributed =  DbtRunOperator(
    #     task_id="fct_sipher_staking_distributed", 
    #     models="fct_sipher_staking_distributed", 
    #     dag=dag
    # )

    staking_pool_transaction >> staking_pool_transaction_agg
    sipher_staking_claimed_rewards
    sipher_staking_distributed
    sipher_staking_rewards_claimed_and_withdrawn