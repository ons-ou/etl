import os
import pickle

import pandas as pd
from datetime import datetime
from scripts.utils.zip_workflow_utils import get_max_year

ELEMENTS = ["CO", "NO2", "Ozone", "PM10", "PM2.5", "SO2"]
STATES = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]

# This one will only be used in case tables are empty
DEFAULT_START_DATE = datetime(1980, 1, 1)

LAST_SELENIUM_DATE = datetime.now()
LAST_ZIP_DATE = datetime(get_max_year(), 1, 1)


def common_transformation(df, max_date, columns_to_keep):
    df['date_local'] = pd.to_datetime(df['date_local'])
    df = df[(df['date_local'] >= pd.to_datetime(max_date)) & (df['aqi'] >= 0) & (df['first_max_value'] >= 0)
            & (df["state_code"] <= 56)]

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
    return df


def insert_aqi_data(db, aqi_df):
    conflict_keys = ["date_local", "latitude", "longitude"]
    # Insert data into aqi_data table
    aqi_columns = list(aqi_df.columns)
    aqi_values = [tuple(row) for row in aqi_df.values]
    db.bulk_insert("aqi_data", aqi_columns, aqi_values, conflict_keys)


def insert_element_data(db, element, element_df, update_columns = None):
    conflict_keys = ["date_local", "latitude", "longitude"]
    # Insert data into element_data table
    element_columns = list(element_df.columns)
    element_values = [tuple(row) for row in element_df.values]
    table_name = f"{str(element).replace('.', '_').lower()}_data"
    db.bulk_insert(table_name, element_columns, element_values, conflict_keys,
                   update_columns=update_columns)
