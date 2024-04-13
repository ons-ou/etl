import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd

from scripts.data_sources.SeleniumDownloader import SeleniumDownloader
from scripts.etl.BaseWorkflow import BaseWorkflow
from scripts.utils.etl_utils import common_transformation, STATES, LAST_SELENIUM_DATE


class SeleniumWorkflow(BaseWorkflow):
    def extract_data(self, element, max_date):
        # TODO: change date here to extract all data
        current_year = LAST_SELENIUM_DATE.year
        max_year = max_date.year

        with ThreadPoolExecutor() as executor:
            futures = []
            for year in range(max_year, current_year + 1):
                for state in STATES:
                    name = f"{element}_{state}_{year}"
                    logging.info(f"Starting Extraction for {name}")

                    directory = os.path.join(self.current_directory, "daily_data", name)
                    # Perform ETL process for the given element, state, and year
                    downloader = SeleniumDownloader(directory)
                    future = executor.submit(downloader.download_data, element, year, state)
                    futures.append((future, directory))

            for future, directory in futures:
                yield os.path.join(directory, future.result())

    @staticmethod
    def transform_data(path, max_date):
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

        df = common_transformation(df, max_date, columns_to_keep)

        aqi_df = df.drop(columns=['first_max_value'])
        co_df = df.drop(columns=['aqi'])

        return aqi_df, co_df
