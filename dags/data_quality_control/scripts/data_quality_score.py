import logging
import time
from datetime import datetime as dt
from logging import getLogger

import numpy as np
import pandas as pd
import pandas_gbq
import pytz
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from google.cloud import bigquery
from google.oauth2 import service_account
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler
from utils.common import set_env_value
from airflow.models import Variable

BIGQUERY_BILLING_PROJECT = Variable.get("bigquery_billing_project", default_var="dev")

EXECUTION_ENVIRONMENT = Variable.get("execution_environment", default_var="dev")

PROJECT_ID = set_env_value(production="sipher-data-platform", dev="sipher-data-testing")
DBT_DATASET_ID = set_env_value(production="dbt_sipher", dev="tmp_dbt")


class DataQualityScore:
    def __init__(self):
        self.EXECUTION_ENVIRONMENT = Variable.get("execution_environment")
        self.BIGQUERY_PROJECT = Variable.get("bigquery_project")
        self.DIMENSIONS = [
            "accuracy",
            "completeness",
            "outlier",
            "usability",
            "freshness",
        ]  # Ranked in order

        self.BINS = {
            "red": 0.7,
            "orange": 0.9,
            "green": 1,
        }

        self.package = {}
        self.service_account_json_path = BaseHook.get_connection(
            "sipher_gcp"
        ).extra_dejson["key_path"]
        self.client = bigquery.Client.from_service_account_json(
            self.service_account_json_path
        )

    def get_list_datasets(self):
        """Takes in a client_id,
        from GBQ
        returns list of dataset
        """
        client_id = self.client
        query_data_extraction = """

                WITH DATASETS AS (
                SELECT catalog_name,schema_name
                FROM `sipher-data-platform.INFORMATION_SCHEMA.SCHEMATA`
                UNION ALL  
                SELECT catalog_name,schema_name
                FROM `sipher-atherlabs-ga.INFORMATION_SCHEMA.SCHEMATA`
                UNION ALL  
                SELECT catalog_name,schema_name
                FROM `sipher-odyssey.INFORMATION_SCHEMA.SCHEMATA`
                UNION ALL  
                SELECT catalog_name,schema_name
                FROM `sipher-website-ga.INFORMATION_SCHEMA.SCHEMATA`
                )

                SELECT * FROM DATASETS 
                WHERE catalog_name = 'sipher-data-platform' 
                AND schema_name = 'sipher_presentation'

                """
        df_list_datasets = client_id.query(
            query_data_extraction, timeout=120, project=BIGQUERY_BILLING_PROJECT
        ).to_dataframe()
        logging.info("done get_list_datasets")
        return df_list_datasets

    def get_list_tables(self, project_id, dataset_id):
        """Takes in a project_id, dataset_id, client_id,
        from GBQ
        returns list of table name"""
        client_id = self.client
        query_data_extraction = """

                SELECT DISTINCT
                    project_id as table_catalog,dataset_id as table_schema,table_id as table_name, TIMESTAMP_MILLIS(last_modified_time) as last_modified_time
                FROM
                    `{0}`.{1}.__TABLES__
                WHERE   table_id NOT LIKE '%SIPHER%'
                """
        df_list_tables = client_id.query(
            query_data_extraction.format(project_id, dataset_id),
            timeout=120,
            project=BIGQUERY_BILLING_PROJECT,
        ).to_dataframe()
        logging.info("done get_list_tables")

        return df_list_tables

    def get_table_suffix(self, project_id, dataset_id, table_id):
        """Takes in a project_id, dataset_id, table_id, client_id
        from GBQ
        returns data suffix table"""

        client_id = self.client

        query_data_extraction = """

                SELECT *
                FROM
                    `{0}.{1}.{2}`
                WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))  
        
                """
        df_table = client_id.query(
            query_data_extraction.format(project_id, dataset_id, table_id),
            timeout=120,
            project=BIGQUERY_BILLING_PROJECT,
        ).to_dataframe()
        logging.info("done get_table_suffix")

        return df_table

    def get_table(self, project_id, dataset_id, table_id):
        """Takes in a project_id, dataset_id, table_id, client_id
        from GBQ
        returns data table"""

        client_id = self.client

        query_data_extraction = """

                SELECT *
                FROM
                    `{0}.{1}.{2}`

        
                """
        df_table = client_id.query(
            query_data_extraction.format(project_id, dataset_id, table_id),
            timeout=120,
            project=BIGQUERY_BILLING_PROJECT,
        ).to_dataframe()

        logging.info("done get_table")

        return df_table

    def get_test_dbt_results_history(self, project_id, dataset_id, table_id):
        """Takes in a project_id, dataset_id, table_id, client_id
        from GBQ
        returns data test case result dbt"""

        client_id = self.client

        query_data_extraction = """

                SELECT *
                FROM
                    `{0}.{1}.{2}`

        
                """
        df_table = client_id.query(
            query_data_extraction.format(project_id, dataset_id, table_id),
            timeout=120,
            project=BIGQUERY_BILLING_PROJECT,
        ).to_dataframe()
        logging.info("done get_test_dbt_results_history")

        return df_table

    def all_projects(self):
        """Takes in a client_id
        returns data of all table each project"""

        list_tables = pd.DataFrame()

        list_datasets = self.get_list_datasets()
        for dataset in list_datasets[["catalog_name", "schema_name"]].itertuples(
            index=True
        ):
            df = self.get_list_tables(dataset.catalog_name, dataset.schema_name)
            list_tables = list_tables.append(df)

        list_tables = list_tables.reset_index().drop("index", axis=1)
        logging.info("done all_projects")

        return list_tables

    def get_dbt_results(self, project_id, dataset_id, table_id):
        """Takes in a project_id, dataset_id, table_id,
        returns data result dbt test"""

        test_dbt_results_history = self.get_test_dbt_results_history(
            project_id, dataset_id, table_id
        )

        test_dbt_results_history["table_name"] = test_dbt_results_history[
            "model_refs"
        ].apply(lambda x: x.split(",")[-1])

        df_dbt = pd.pivot_table(
            test_dbt_results_history[["table_name", "test_result", "sk_id"]],
            values="sk_id",
            index="table_name",
            columns=["test_result"],
            fill_value=0,
            aggfunc="count",
        ).reset_index()

        df_dbt["accuracy"] = df_dbt["pass"] / (df_dbt["pass"] + df_dbt["fail"])
        logging.info("done get_dbt_results")
        return df_dbt

    def score_freshness(self, data):
        """
        How up to date is the data?
        """
        lag = (
            pd.to_datetime(dt.utcnow(), utc=True)
            - pd.to_datetime(data.last_modified_time, utc=True)
        ).days

        freshness = 1 - (lag) / 2
        logging.info("done score_freshness")

        return freshness

    def outlier_scores(self, data):
        """Takes in a data
        returns score outlier"""

        data = data.drop("freshness", axis=1)
        data = data[data.columns.drop(list(data.filter(regex="id|timestamp")))]

        non_floats = []

        for col in data.columns:
            try:
                data[col] = pd.to_numeric(data[col])
            except Exception as exp:
                non_floats.append(col)
                getLogger("warning").warning(
                    "CANNOT CAST NUMERIC " + col, exc_info=True
                )

        data = data.drop(columns=non_floats)
        data.dropna(inplace=True)
        if data.empty:
            getLogger("warning").warning("outlier_scores() DataFrame is empty!")
            return 1
        else:
            di = data.shape[1]
            ################### Train Isolation Forest #################
            model = IsolationForest(
                n_estimators=50,
                max_samples="auto",
                contamination=0.05,
                max_features=di,
                bootstrap=False,
                n_jobs=1,
                random_state=42,
                verbose=0,
                warm_start=False,
            ).fit(data)

            # Get Anomaly Scores and Predictions
            # anomaly_score = model.decision_function(data)
            # predictions = model.score_samples(data)
            original_paper_score = abs(model.score_samples(data))
            # return 1 - (len(np.extract(predictions==-1 , predictions))/len(predictions))
            logging.info("done score_freshness")
            return max(
                1 - np.mean(original_paper_score), 1 - np.median(original_paper_score)
            )

    def score_usability(self, data):
        """Takes in a data
        returns score usability"""
        score = []
        for column in data:
            try:
                data[column] = pd.to_numeric(data[column])
            except:
                pass

        if len(data[column]) > 0:
            fml = 1 - (
                np.count_nonzero(np.array(data[column]) == 0.0)
                * 1.0
                / len(data[column])
            )
        else:
            fml = 0
        score.append(fml)
        # np.mean(score)
        logging.info("done score_usability")

        return np.mean(score)

    # https://github.com/open-data-toronto/framework-data-quality/blob/master/data_quality_score.ipynb

    def calculate_weights(self, method="sr"):
        """returns weights of dimension"""
        dimensions = self.DIMENSIONS
        N = len(dimensions)
        if method == "sr":
            denom = np.array(
                [
                    ((1 / (i + 1)) + ((N + 1 - (i + 1)) / N))
                    for i, x in enumerate(dimensions)
                ]
            ).sum()
            weights = [
                ((1 / (i + 1)) + ((N + 1 - (i + 1)) / N)) / denom
                for i, x in enumerate(dimensions)
            ]
        elif method == "rs":
            denom = np.array(
                [(N + 1 - (i + 1)) for i, x in enumerate(dimensions)]
            ).sum()
            weights = [(N + 1 - (i + 1)) / denom for i, x in enumerate(dimensions)]
        elif method == "rr":
            denom = np.array([1 / (i + 1) for i, x in enumerate(dimensions)]).sum()
            weights = [(1 / (i + 1)) / denom for i, x in enumerate(dimensions)]
        elif method == "re":
            exp = 0.2
            denom = np.array(
                [(N + 1 - (i + 1)) ** exp for i, x in enumerate(dimensions)]
            ).sum()
            weights = [
                (N + 1 - (i + 1)) ** exp / denom for i, x in enumerate(dimensions)
            ]
        else:
            raise Exception("Invalid weighting method provided")
        logging.info("done calculate_weights")

        return weights

    def score_completeness(self, data):
        """
        How much of the data is missing?
        """
        logging.info("done score_completeness")
        return 1 - (np.sum(len(data) - data.count()) / np.prod(data.shape))

    def wrap_data_into_package(self):
        list_tables = self.all_projects()

        for row in list_tables.itertuples(index=True):
            try:
                dt.strptime(row.table_name[-8::1], "%Y%m%d").strftime("%Y%m%d")
                # logging.info(list_tables.at[row.Index, "table_name"])
                list_tables.at[row.Index, "table_name"] = row.table_name[:-8:1] + "*"

            except:
                pass
        list_tables = list_tables.drop_duplicates()

        df_dbt = self.get_dbt_results(
            project_id=PROJECT_ID,
            dataset_id=DBT_DATASET_ID,
            table_id="test_results_history",
        )

        package = {}

        for row in list_tables.itertuples(index=False):
            df = pd.DataFrame()

            try:
                if "*" in row.table_name:
                    df = self.get_table_suffix(
                        row.table_catalog, row.table_schema, row.table_name
                    )
                else:
                    df = self.get_table(
                        row.table_catalog, row.table_schema, row.table_name
                    )

            except Exception as exp:
                getLogger("warning").warning(
                    "CANNOT GET TABLE " + row.table_name, exc_info=True
                )

            pkg_key = row.table_catalog + "," + row.table_schema + "," + row.table_name

            df["freshness"] = self.score_freshness(row)
            df["accuracy"] = df_dbt[df_dbt["table_name"] == row.table_name]["accuracy"]

            package[pkg_key] = df

        self.package = package

    def score_each_table(self, df):
        """takes in a dataframe
        returns score each table"""

        score = []

        accuracy = np.mean(df.accuracy)
        completeness = self.score_completeness(df)
        if completeness >= 0.8:
            outlier = self.outlier_scores(df)
        else:
            outlier = np.nan
        usability = self.score_usability(df)
        freshness = max(np.mean(df.freshness), 0)

        score.append(accuracy)
        score.append(outlier)
        score.append(completeness)
        score.append(usability)
        score.append(freshness)
        # logging.info(score)
        df_scores = pd.DataFrame(score).transpose().fillna(0)
        df_scores.columns = self.DIMENSIONS
        logging.info("done score_each_table")

        return df_scores

    def score_catalogue(self):
        """takes in a dictionary
        returns score dataframe"""

        dimensions = self.DIMENSIONS
        package = self.package

        df_scores_all_table = pd.DataFrame()

        for key, value in package.items():
            # logging.info(key)
            df_one_table = pd.DataFrame()
            df_one_table = self.score_each_table(value).copy()
            df_one_table.insert(loc=0, column="table", value=key)
            df_scores_all_table = df_scores_all_table.append(df_one_table)

        labels = list(self.BINS.keys())
        weights = self.calculate_weights()
        df_scores_all_table["score"] = (
            df_scores_all_table[dimensions].mul(weights).sum(1)
        )

        df_scores_all_table["score_norm"] = MinMaxScaler().fit_transform(
            df_scores_all_table[["score"]]
        )
        bins = [-1]
        bins.extend(self.BINS.values())
        df_scores_all_table["grade"] = pd.cut(
            df_scores_all_table["score"], bins=bins, labels=labels
        )
        df_scores_all_table["grade_norm"] = pd.cut(
            df_scores_all_table["score_norm"], bins=bins, labels=labels
        )

        UTC = pytz.utc
        df_scores_all_table["recorded_at"] = dt.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        df_scores_all_table = df_scores_all_table.round(2)

        df_scores_all_table["table_name"] = df_scores_all_table["table"].apply(
            lambda x: x.split(",")[-1]
        )
        df_scores_all_table["dataset"] = df_scores_all_table["table"].apply(
            lambda x: x.split(",")[1]
        )
        df_scores_all_table["project"] = df_scores_all_table["table"].apply(
            lambda x: x.split(",")[0]
        )

        logging.info("done score_catalogue")

        return df_scores_all_table

    def run(self):
        self.wrap_data_into_package()
        final_df = self.score_catalogue()

        final_df["recorded_at"] = pd.to_datetime(
            final_df["recorded_at"], format="%Y-%m-%d %H:%M:%S"
        )

        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json_path
        )

        pandas_gbq.context.credentials = credentials

        """sync score dataframe into GBQ"""
        start = time.time()
        pandas_gbq.to_gbq(
            final_df,
            "data_quality_control.data_quality_score",
            project_id=PROJECT_ID,
            if_exists="append",
        )
        logging.info("done to_gbq")

        end = time.time()
        logging.info("time alternative 3 " + str(end - start))
