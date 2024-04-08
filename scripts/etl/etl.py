import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pandas as pd
from scripts.data_sources.EPAAirQualityDownloader import EPAAirQualityDownloader
from scripts.utils.Database import Database

current_directory = os.path.dirname(os.path.dirname(os.getcwd()))

ELEMENT_COLUMNS = {
    "date_local": "DATE",
    "first_max_value": "NUMERIC",
    "local_site_name": "VARCHAR(255)",
    "observation_count": "INTEGER",
    "state_code": "VARCHAR(2)",
    "county_code": "VARCHAR(3)",
    "latitude": "NUMERIC",
    "longitude": "NUMERIC",
    "season": "VARCHAR(255)",
    "day_of_week": "INTEGER",
}

AQI_COLUMNS = {
    "date_local": "DATE",
    "aqi": "NUMERIC",
    "local_site_name": "VARCHAR(255)",
    "observation_count": "INTEGER",
    "state_code": "VARCHAR(2)",
    "county_code": "VARCHAR(3)",
    "latitude": "NUMERIC",
    "longitude": "NUMERIC",
    "season": "VARCHAR(255)",
    "day_of_week": "INTEGER",
}

ELEMENTS = ["CO"]
STATES = ["Arizona"]


def workflow_init():
    db = Database()
    db.connect()
    # Create AQI data table
    db.create_table("aqi_data", AQI_COLUMNS, primary_keys=["date_local", "state_code", "county_code", "latitude", "longitude"])

    for element in ELEMENTS:
        # Create element data table
        db.create_table(f"{str(element).lower()}_data", ELEMENT_COLUMNS,
                        primary_keys=["date_local", "state_code", "county_code", "latitude", "longitude"])

    db.disconnect()

    for element in ELEMENTS:
        db.connect()
        max_date = db.get_max_date(f"{str(element).lower()}_data", "date_local")
        db.disconnect()
        if max_date is None:
            yield element, datetime(1980, 1, 1)
        else:
            yield element, max_date + timedelta(days=1)


def extract_data(element, max_date):
    current_year = datetime.now().year
    max_year = max_date.year

    with ThreadPoolExecutor() as executor:
        futures = []
        for year in range(max_year, current_year + 1):
            for state in STATES:
                name = f"{element}_{state}_{year}"
                logging.info(f"Starting Extraction for {name}")

                directory = os.path.join(current_directory, "daily_data", name)
                # Perform ETL process for the given element, state, and year
                downloader = EPAAirQualityDownloader(directory)
                future = executor.submit(downloader.download_data, element, state, year)
                futures.append((future, directory))

        for future, directory in futures:
            yield os.path.join(directory, future.result())



def transform_data(path, max_date):
    df = pd.read_csv(path)
    df.columns = [
        "date_local", "source", "site_id", "poc", "first_max_value", "units", "aqi", "local_site_name", "observation_count",
        "observation_percent", "parameter_code", "parameter_name", "cbsa_code", "cbsa_name", "state_code", "state_name",
        "county_code", "county_name", "latitude", "longitude"
    ]

    df['date_local'] = pd.to_datetime(df['date_local'])
    df = df[df['date_local'] >= pd.to_datetime(max_date)]

    columns_to_keep = [
        "date_local", "first_max_value", "aqi", "local_site_name", "observation_count",
        "state_code", "county_code", "latitude", "longitude",
    ]

    df = df[columns_to_keep]

    df = df[
        (df['latitude'].apply(lambda x: 19.50139 <= x <= 64.85694)) &
        (df['longitude'].apply(lambda x: -161.75583 <= x <= -68.01197))
        ]

    df = df.dropna(subset=['aqi'])

    df['day_of_week'] = df['date_local'].dt.dayofweek

    def get_season(month):
        if month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        elif month in [9, 10, 11]:
            return 'Fall'
        else:
            return 'Winter'

    df['season'] = df['date_local'].dt.month.apply(get_season)

    aqi_df = df.drop(columns=['first_max_value'])
    co_df = df.drop(columns=['aqi'])

    return aqi_df, co_df


def load_data(aqi_df, element_df, element):
    db = Database()
    db.connect()

    # Insert data into aqi_data table
    for _, row in aqi_df.iterrows():
        logging.info(row)
        columns = list(row.index)
        values = list(row.values)
        db.insert_data(f"aqi_data", columns, values)

    # Insert data into element_data table
    for _, row in element_df.iterrows():
        columns = list(row.index)
        values = list(row.values)

    db.disconnect()


