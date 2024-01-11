from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from typing import Dict
from utils.data_upload.bigquery_upload import create_external_bq_table_to_gcs
from raw_gaming_chain_coinmarketcap_crawl.tasks.get_details_coin.script import GetCoinMarketCapGamingData
from datetime import timedelta, datetime
import logging
import pandas as pd


class TaskGroupRequiredCoinsDetails(TaskGroup):
     
    def __init__(
        self,
        group_id: str,
        bq_project: str,
        bq_dataset: str,
        gcs_bucket: str,
        gcp_conn_id: str,
        tooltip: str = "Get List Coins From Coin MarketCap",
        **kwargs
    ):
        super().__init__(
            group_id=group_id,
            tooltip=tooltip,
            **kwargs
        )
    
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

        
        @task(task_group=self)
        def create_big_lake_table_raw_coinmarketcap_btc_eth_sipher_one_year():

            partition_expr = "{snapshot_timestamp:TIMESTAMP}"

            return create_external_bq_table_to_gcs(
                gcp_conn_id        = self.gcp_conn_id,
                bq_project         = self.bq_project,
                bq_dataset         = self.bq_dataset,
                bq_table           = 'raw_coinmarketcap_btc_eth_sipher_one_year',
                gcs_bucket         = self.gcs_bucket,
                gcs_object_prefix  = "raw_coinmarketcap/btc_eth_sipher",
                gcs_partition_expr = partition_expr,
            )
        
        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_btc_coins_one_year_price(**kwargs):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/bitcoin/',
                day_range='1Y',
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/btc_eth_sipher",
            ).execute_task()

        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_eth_coins_one_year_price(**kwargs):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/ethereum/',
                day_range='1Y',
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/btc_eth_sipher",
            ).execute_task()

        @task(task_group=self, pool="raw_gaming_chain_coinmarketcap_crawl_pool")
        def get_sipher_coins_one_year_price(**kwargs):
            ds = kwargs['ds']
            execution_date = kwargs['execution_date']

            return GetCoinMarketCapGamingData(
                website='https://coinmarketcap.com/currencies/sipher/',
                day_range='1Y',
                ds=ds,
                timestamp=int(execution_date.timestamp()),
                gcs_bucket=self.gcs_bucket,
                gcs_prefix="raw_coinmarketcap/btc_eth_sipher",
            ).execute_task()


        [
            
            get_btc_coins_one_year_price(),
            get_eth_coins_one_year_price(),
            get_sipher_coins_one_year_price()

        ] >> create_big_lake_table_raw_coinmarketcap_btc_eth_sipher_one_year()
