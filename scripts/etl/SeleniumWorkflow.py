import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pandas as pd

from scripts.data_sources.SeleniumDownloader import SeleniumDownloader
from scripts.etl.BaseWorkflow import BaseWorkflow
from scripts.utils.Database import Database
from scripts.utils.etl_utils import common_transformation, STATES, LAST_SELENIUM_DATE, insert_aqi_data, \
    insert_element_data, DEFAULT_START_DATE, ELEMENT_CODES


class SeleniumWorkflow(BaseWorkflow):
    @staticmethod
    def workflow_init(element):
        db = BaseWorkflow.workflow_init(element)
        table_name = str(ELEMENT_CODES[element]).replace('.', '_').lower()+"_data"
        max_date = db.get_max_query(table_name, "date_local")
        db.disconnect()
        if max_date is None:
            return element, DEFAULT_START_DATE
        else:
            return element, max_date + timedelta(days=1)

    def extract_data(self, element, max_date):
        current_year = LAST_SELENIUM_DATE.year
        max_year = max_date.year

        with ThreadPoolExecutor() as executor:
            futures = []
            for year in range(max_year, current_year + 1):
                for state in STATES:
                    name = f"{element}_{state}_{year}"
                    logging.info(f"Starting Extraction for {name}")

                    directory = os.path.join(self.current_directory, "daily_data_selenium")
                    # Perform ETL process for the given element, state, and year
                    downloader = SeleniumDownloader(directory)
                    new_name = os.path.join(directory, f"{name}.csv")
                    future = executor.submit(downloader.download_data, element, year, state)
                    futures.append((future, directory, new_name))

            for future, directory, new_name in futures:
                old_name = os.path.join(directory, future.result())
                os.rename(old_name, new_name)
                yield new_name

    @staticmethod
    def transform_data(path, max_date, element):
        df = pd.read_csv(path)
        df.columns = [
            "date_local", "source", "site_id", "poc", "first_max_value", "units", "aqi", "local_site_name", "observation_count",
            "observation_percent", "parameter_code", "parameter_name", "cbsa_code", "cbsa_name", "state_code", "state_name",
            "county_code", "county_name", "latitude", "longitude"
        ]

        columns_to_keep = [
            "date_local", "first_max_value", "aqi", "observation_count",
            "state_code", "county_code", "latitude", "longitude",
        ]

        df = common_transformation(df, max_date, columns_to_keep, element)

        aqi_df = df.drop(columns=['first_max_value', 'element_category'])
        aqi_df.rename(columns={'aqi_category': 'category'}, inplace=True)

        co_df = df.drop(columns=['aqi', 'aqi_category'])
        co_df.rename(columns={'element_category': 'category'}, inplace=True)

        return aqi_df, co_df

    @staticmethod
    def load_data(aqi_df, element_df, element):
        db = Database()
        db.connect()

        insert_aqi_data(db, aqi_df)
        insert_element_data(db, element, element_df)

        db.disconnect()