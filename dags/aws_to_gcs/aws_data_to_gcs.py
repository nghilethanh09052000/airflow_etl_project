from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow.utils.task_group import TaskGroup

from aws_to_gcs.scripts.ather_id import AtherId
from aws_to_gcs.scripts.aws_billing import AwsBilling
from aws_to_gcs.scripts.loyalty_dashboard import Loyalty
from aws_to_gcs.scripts.onchain_nft import AwsOnchainNFT
from utils.alerting.airflow import airflow_callback
from utils.common import set_env_value
from utils.dbt import default_args_for_dbt_operators

start_date = set_env_value(production=datetime(2022, 11, 3), dev=datetime(2022, 12, 19))
schedule_interval = set_env_value(production="@daily", dev="@once")
BIGQUERY_PROJECT = Variable.get("bigquery_project")

DEFAULT_ARGS = {
    "owner": "tri.nguyen",
    "start_date": start_date,
    "trigger_rule": "all_done",
    "on_failure_callback": airflow_callback,
}

DEFAULT_ARGS.update(default_args_for_dbt_operators)

with DAG(
    "aws_data_to_gcs",
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    catchup=False,
    tags=["aws", "gcs", "ather_id", "loyalty", "billing"]
    ) as dag:

    with TaskGroup(group_id="ather_id", prefix_group_id=False) as ather_id:

        ather_id_gcs = PythonOperator(
            task_id="ather_id_gcs",
            dag=dag, provide_context=True,
            python_callable=AtherId.airflow_callable
        )

        stg_aws__ather_id__raw_wallet =  DbtRunOperator(
            task_id="stg_aws__ather_id__raw_wallet", 
            models="stg_aws__ather_id__raw_wallet", 
            dag=dag
        )

        stg_aws__ather_id__raw_cognito =  DbtRunOperator(
            task_id="stg_aws__ather_id__raw_cognito", 
            models="stg_aws__ather_id__raw_cognito", 
            dag=dag
        )

        stg_aws__ather_id__raw_user =  DbtRunOperator(
            task_id="stg_aws__ather_id__raw_user", 
            models="stg_aws__ather_id__raw_user", 
            dag=dag
        )

        dim_ather_user__all =  DbtRunOperator(
            task_id="dim_ather_user__all", 
            models="dim_ather_user__all", 
            dag=dag
        )

        dim_ather_user__wallet =  DbtRunOperator(
            task_id="dim_ather_user__wallet", 
            models="dim_ather_user__wallet", 
            dag=dag
        )

        raw_dim_user_all = BigQueryExecuteQueryOperator(
            task_id="raw_dim_user_all",
            use_legacy_sql=False,
            gcp_conn_id="sipher_gcp",
            sql="query/raw_dim_user_all.sql",
            params={'bigquery_project': BIGQUERY_PROJECT},
            dag=dag,
        )

        raw_dim_user_wallet = BigQueryExecuteQueryOperator(
            task_id="raw_dim_user_wallet",
            use_legacy_sql=False,
            gcp_conn_id="sipher_gcp",
            sql="query/raw_dim_user_wallet.sql",
            params={'bigquery_project': BIGQUERY_PROJECT},
            dag=dag,
        )

        ather_id_gcs >> [stg_aws__ather_id__raw_wallet, stg_aws__ather_id__raw_cognito, stg_aws__ather_id__raw_user] >> dim_ather_user__wallet >>raw_dim_user_wallet
        ather_id_gcs >> [stg_aws__ather_id__raw_wallet, stg_aws__ather_id__raw_cognito, stg_aws__ather_id__raw_user] >> dim_ather_user__all >> raw_dim_user_all

    with TaskGroup(group_id="loyalty_dashboard", prefix_group_id=False) as loyalty_dashboard:

        loyalty_dashboard_gcs = PythonOperator(
            task_id="loyalty_dashboard_gcs",
            dag=dag,
            provide_context=True,
            python_callable=Loyalty.airflow_callable,
        )

        stg_aws__loyalty__raw_burned =  DbtRunOperator(
            task_id="stg_aws__loyalty__raw_burned", 
            models="stg_aws__loyalty__raw_burned", 
            dag=dag
        )

        stg_aws__loyalty__raw_log_claim_lootbox =  DbtRunOperator(
            task_id="stg_aws__loyalty__raw_log_claim_lootbox", 
            models="stg_aws__loyalty__raw_log_claim_lootbox", 
            dag=dag
        )

        stg_aws__loyalty__raw_log_open_lootbox =  DbtRunOperator(
            task_id="stg_aws__loyalty__raw_log_open_lootbox", 
            models="stg_aws__loyalty__raw_log_open_lootbox", 
            dag=dag
        )

        stg_aws__loyalty__raw_log_scrap_spaceship_parts =  DbtRunOperator(
            task_id="stg_aws__loyalty__raw_log_scrap_spaceship_parts", 
            models="stg_aws__loyalty__raw_log_scrap_spaceship_parts", 
            dag=dag
        )

        stg_aws__loyalty__raw_log_spaceship =  DbtRunOperator(
            task_id="stg_aws__loyalty__raw_log_spaceship", 
            models="stg_aws__loyalty__raw_log_spaceship", 
            dag=dag
        )

        stg_aws__loyalty__raw_pending_mint =  DbtRunOperator(
            task_id="stg_aws__loyalty__raw_pending_mint", 
            models="stg_aws__loyalty__raw_pending_mint", 
            dag=dag
        )

        loyalty_dashboard_gcs >> [stg_aws__loyalty__raw_burned, stg_aws__loyalty__raw_log_claim_lootbox, stg_aws__loyalty__raw_log_open_lootbox, stg_aws__loyalty__raw_log_scrap_spaceship_parts, stg_aws__loyalty__raw_log_spaceship, stg_aws__loyalty__raw_pending_mint]

    with TaskGroup(group_id="aws_onchain_nft", prefix_group_id=False) as aws_onchain_nft:

        aws_onchain_nft_gcs = PythonOperator(
            task_id="aws_onchain_nft_gcs",
            dag=dag,
            provide_context=True,
            python_callable=AwsOnchainNFT.airflow_callable,
        )

        stg_opensearch_onchain__raw_lootbox =  DbtRunOperator(
            task_id="stg_opensearch_onchain__raw_lootbox", 
            models="stg_opensearch_onchain__raw_lootbox", 
            dag=dag
        )

        stg_opensearch_onchain__raw_sculpture =  DbtRunOperator(
            task_id="stg_opensearch_onchain__raw_sculpture", 
            models="stg_opensearch_onchain__raw_sculpture", 
            dag=dag
        )

        stg_opensearch_onchain__raw_spaceship =  DbtRunOperator(
            task_id="stg_opensearch_onchain__raw_spaceship", 
            models="stg_opensearch_onchain__raw_spaceship", 
            dag=dag
        )

        stg_opensearch_onchain__raw_spaceship_parts =  DbtRunOperator(
            task_id="stg_opensearch_onchain__raw_spaceship_parts", 
            models="stg_opensearch_onchain__raw_spaceship_parts", 
            dag=dag
        )

        aws_onchain_nft_gcs >> [stg_opensearch_onchain__raw_lootbox, stg_opensearch_onchain__raw_sculpture, stg_opensearch_onchain__raw_spaceship, stg_opensearch_onchain__raw_spaceship_parts]

    with TaskGroup(group_id="aws_billing", prefix_group_id=False) as aws_billing:

        aws_billing_gcs = PythonOperator(
            task_id="aws_billing_gcs",
            dag=dag,
            provide_context=True,
            python_callable=AwsBilling.airflow_callable,
        )

        stg_aws__billing__raw_blockchain =  DbtRunOperator(
            task_id="stg_aws__billing__raw_blockchain", 
            models="stg_aws__billing__raw_blockchain", 
            dag=dag
        )

        stg_aws__billing__raw_g1 =  DbtRunOperator(
            task_id="stg_aws__billing__raw_g1", 
            models="stg_aws__billing__raw_g1", 
            dag=dag
        )

        stg_aws__billing__raw_marketplace =  DbtRunOperator(
            task_id="stg_aws__billing__raw_marketplace", 
            models="stg_aws__billing__raw_marketplace", 
            dag=dag
        )

        stg_aws__billing__raw_marketplace =  DbtRunOperator(
            task_id="stg_aws__billing__raw_game_production", 
            models="stg_aws__billing__raw_game_production", 
            dag=dag
        )

        stg_aws__billing__raw_metaverse =  DbtRunOperator(
            task_id="stg_aws__billing__raw_metaverse", 
            models="stg_aws__billing__raw_metaverse", 
            dag=dag
        )

        aws_billing_gcs >> [stg_aws__billing__raw_blockchain, stg_aws__billing__raw_g1, stg_aws__billing__raw_marketplace, stg_aws__billing__raw_marketplace]

    
    fct_atherlabs_users_assets =  DbtRunOperator(
        task_id="fct_atherlabs_users_assets", 
        models="fct_atherlabs_users_assets", 
        dag=dag
    )

    fct_sipher_other_token_owners =  DbtRunOperator(
        task_id="fct_sipher_other_token_owners", 
        models="fct_sipher_other_token_owners", 
        dag=dag
    )

    fct_aws_billing_all =  DbtRunOperator(
        task_id="fct_aws_billing_all", 
        models="fct_aws_billing_all", 
        dag=dag
    )

    atherlabs_users_asset = BigQueryExecuteQueryOperator(
        task_id="atherlabs_users_asset",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/atherlabs_users_asset.sql",
        params={'bigquery_project': BIGQUERY_PROJECT},
        dag=dag,
    )

    sipher_other_token_owners = BigQueryExecuteQueryOperator(
        task_id="sipher_other_token_owners",
        use_legacy_sql=False,
        gcp_conn_id="sipher_gcp",
        sql="query/sipher_other_token_owners.sql",
        params={'bigquery_project': BIGQUERY_PROJECT},
        dag=dag,
    )

    ather_id >> fct_atherlabs_users_assets >> atherlabs_users_asset
    loyalty_dashboard >> fct_sipher_other_token_owners >> sipher_other_token_owners
    aws_billing >> fct_aws_billing_all