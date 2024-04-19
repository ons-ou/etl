import os
import pickle

import pandas as pd
from datetime import datetime
from scripts.utils.zip_workflow_utils import get_max_year

ELEMENT_CODES = {
    "42101": "CO",
    "42401": "SO2",
    "44201": "Ozone",
    "42602": "NO2",
    "88101": "PM2.5",
    "81102": "PM10"
}

ELEMENTS = ["42101", "42401", "44201", "42602", "88101", "81102"]
STATES = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21',
          '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
          '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56']

# This one will only be used in case tables are empty
DEFAULT_START_DATE = datetime(1980, 1, 1)

LAST_SELENIUM_DATE = datetime.now()
LAST_ZIP_DATE = datetime(get_max_year(), 1, 1)


def common_transformation(df, max_date, columns_to_keep, element):
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

    # Define a function to map AQI values to categories
    def get_aqi_category(aqi):
        if 0 <= aqi <= 50:
            return 'Good'
        elif 51 <= aqi <= 100:
            return 'Moderate'
        elif 101 <= aqi <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif 151 <= aqi <= 200:
            return 'Unhealthy'
        elif 201 <= aqi <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'

    # Apply the function to create a new column 'aqi_category'
    df['aqi_category'] = df['aqi'].apply(get_aqi_category)

    # Define a function to map element values to categories
    def get_element_category(value, element):
        # Define thresholds for each element
        thresholds = {
            '42101': [0, 4.4, 9, 12, 24, 49, 99, 199, 399, float('inf')],
            '42401': [0, 35, 75, 185, 304, 604, float('inf')],
            '42602': [0, 53, 100, 360, 649, 1249, float('inf')],
            '81102': [0, 54, 154, 254, 354, 424, float('inf')],
            '88101': [0, 12, 35.4, 55.4, 150.4, 250.4, float('inf')],
            '44201': [0, 54, 70, 85, 105, 200, float('inf')]
        }

        categories = ['Normal', 'No health effects', 'Mild', 'Moderate', 'Unhealthy', 'Very Unhealthy', 'Hazardous']

        # Find the category for the given value and element
        for i in range(len(thresholds[element])):
            if value <= thresholds[element][i]:
                return categories[i]
        return categories[-1]  # Return the last category if value exceeds all thresholds

    # Apply the function to create a new column 'element_category'
    df['element_category'] = df['first_max_value'].apply(lambda x: get_element_category(x, element))
    return df


def insert_aqi_data(db, aqi_df):
    conflict_keys = ["date_local", "latitude", "longitude"]
    # Insert data into aqi_data table
    aqi_columns = list(aqi_df.columns)
    aqi_values = [tuple(row) for row in aqi_df.values]
    db.bulk_insert("aqi_data", aqi_columns, aqi_values, conflict_keys)


def insert_element_data(db, element, element_df, update_columns=None):
    conflict_keys = ["date_local", "latitude", "longitude"]
    # Insert data into element_data table
    element_columns = list(element_df.columns)
    element_values = [tuple(row) for row in element_df.values]
    table_name = f"{str(ELEMENT_CODES[element]).replace('.', '_').lower()}_data"
    db.bulk_insert(table_name, element_columns, element_values, conflict_keys,
                   update_columns=update_columns)
