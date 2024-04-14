import logging
import os
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import timedelta

import pandas as pd

from scripts.data_sources.ZipDownloader import ZipDownloader
from scripts.etl.BaseWorkflow import BaseWorkflow
from scripts.utils.Database import Database
from scripts.utils.etl_utils import common_transformation, LAST_ZIP_DATE, insert_element_data, insert_aqi_data, \
    DEFAULT_START_DATE
from scripts.utils.zip_workflow_utils import get_last_update_date, get_current_site_update, update_cache


class ZipWorkflow(BaseWorkflow):
    @staticmethod
    def workflow_init(element):
        db = BaseWorkflow.workflow_init(element)
        table_name = str(element).replace('.', '_').lower()+"_data"
        max_date = db.get_max_query(table_name, "date_local", where_condition="first_max_hour IS NOT NULL")
        db.disconnect()
        if max_date is None:
            return element, DEFAULT_START_DATE
        else:
            return element, max_date + timedelta(days=1)

    def extract_data(self, element, max_date):
        current_year = LAST_ZIP_DATE.year
        max_year = max_date.year

        with ThreadPoolExecutor() as executor:
            futures = []
            for year in range(max_year, current_year + 1):
                name = f"{element}_{year}"
                logging.info(f"Starting Extraction for {name}")

                directory = os.path.join(self.current_directory, "daily_data_zip")
                # Perform ETL process for the given element, state, and year
                downloader = ZipDownloader(directory)
                future = executor.submit(downloader.download_data, element, year)
                futures.append((future, directory))

            for future, directory in futures:
                yield os.path.join(directory, future.result())

    @staticmethod
    def transform_data(path, max_date):
        df = pd.read_csv(path, low_memory=False)
        df.columns = [
            "state_code", "county_code", "site_num", "parameter_code", "poc", "latitude", "longitude", "datum",
            "parameter_name", "sample_duration", "pollutant_standard", "date_local", "uom", "event_type",
            "observation_count",
            "observation_percent", "arithmetic_mean", "first_max_value", "first_max_hour", "aqi", "method_code"
            , "method_name", "local_site_name", "address", "state_name", "county_name", "city_name", "cbsa_name",
            "date_of_last_change"
        ]

        columns_to_keep = [
            "date_local", "first_max_value", "aqi", "observation_count",
            "state_code", "county_code", "latitude", "longitude", "arithmetic_mean", "first_max_hour"
        ]

        df = common_transformation(df, max_date, columns_to_keep)

        aqi_df = df.drop(columns=['first_max_value', "first_max_hour", "arithmetic_mean"])
        co_df = df.drop(columns=['aqi'])

        return aqi_df, co_df

    @staticmethod
    def load_data(aqi_df, element_df, element):
        db = Database()
        db.connect()

        insert_aqi_data(db, aqi_df)

        insert_element_data(db, element, element_df, ["arithmetic_mean", "first_max_value"])

        db.disconnect()

    def workflow_thread(self):
        if get_last_update_date() == get_current_site_update():
            logging.info("Up to date")
            return
        futures = super().workflow_thread()
        wait(futures)

        # All threads are completed, update the cache
        update_cache()

