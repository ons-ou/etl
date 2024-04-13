import logging
import os
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime

import pandas as pd

from scripts.data_sources.ZipDownloader import ZipDownloader
from scripts.etl.BaseWorkflow import BaseWorkflow
from scripts.utils.etl_utils import common_transformation, LAST_ZIP_DATE, get_last_update_date, get_current_site_update, \
    update_cache


class ZipWorkflow(BaseWorkflow):
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

    def workflow_thread(self):
        if get_last_update_date() == get_current_site_update():
            logging.info("Up to date")
            return
        futures = super().workflow_thread()
        wait(futures)

        # All threads are completed, update the cache
        update_cache()

