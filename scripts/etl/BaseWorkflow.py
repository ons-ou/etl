import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from scripts.utils.Database import Database
from scripts.utils.etl_utils import ELEMENTS, DEFAULT_START_DATE
from scripts.utils.table_columns import AQI_COLUMNS, ELEMENT_COLUMNS


class BaseWorkflow:
    def __init__(self):
        self.current_directory = os.path.dirname(os.path.dirname(os.getcwd()))

    @staticmethod
    def workflow_init(element):
        logging.info(f"Starting Workflow for {element}")
        db = Database()
        db.connect()
        foreign_keys = [
            ("fk_state_county", "counties", ["state_code", "county_code"], ["state_code", "county_code"])
            ]
        if not db.table_exists("aqi_data"):
            # Create AQI data table
            db.create_table("aqi_data", AQI_COLUMNS,
                        primary_keys=["date_local", "latitude", "longitude"],
                        foreign_keys= foreign_keys)

            db.create_month_year_index("aqi_data")
        # Create element data table
        table_name = str(element).replace('.', '_').lower()+"_data"
        db.create_table(table_name, ELEMENT_COLUMNS,
                        primary_keys=["date_local", "latitude", "longitude"],
                        foreign_keys= foreign_keys)

        max_date = db.get_max_date(table_name, "date_local")
        db.create_month_year_index(table_name)
        db.disconnect()
        if max_date is None:
            return element, DEFAULT_START_DATE
        else:
            return element, max_date + timedelta(days=1)

    def extract_data(self, element, max_date):
        raise NotImplementedError("Subclass must implement abstract method")

    @staticmethod
    def transform_data(path, max_date):
        raise NotImplementedError("Subclass must implement abstract method")

    @staticmethod
    def load_data(aqi_df, element_df, element):
        db = Database()
        db.connect()

        conflict_keys = ["date_local", "latitude", "longitude"]
        # Insert data into aqi_data table
        aqi_columns = list(aqi_df.columns)
        aqi_values = [tuple(row) for row in aqi_df.values]
        db.bulk_insert("aqi_data", aqi_columns, aqi_values, conflict_keys)

        # Insert data into element_data table
        element_columns = list(element_df.columns)
        element_values = [tuple(row) for row in element_df.values]
        table_name = f"{str(element).replace('.', '_').lower()}_data"
        db.bulk_insert(table_name, element_columns, element_values, conflict_keys)

        db.disconnect()

    def transform_data_and_load(self, element, path, max_date):
        if path is not None:
            logging.info(f"Starting transforming data for {element} from {path}")
            aqi_df, co_df = self.transform_data(path, max_date)
            logging.info(f"Starting Loading data for {element} from {path}")
            self.load_data(aqi_df, co_df, element)
            logging.info(f"Loaded data for {element} from {path}")

    def workflow_thread(self):
        with ThreadPoolExecutor() as executor:
            futures = []
            for element in ELEMENTS:
                future = executor.submit(self.workflow_init, element)
                futures.append(future)

            for future in as_completed(futures):
                element, max_date = future.result()
                if max_date is not None:
                    logging.info(f"Starting Extraction for {element} after {max_date}")
                    for path in self.extract_data(element, max_date):
                        logging.info(f"Extracted data to {path}")
                        executor.submit(self.transform_data_and_load, element, path, max_date)

            return futures