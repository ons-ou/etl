import os
import pickle

import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

import requests

ELEMENTS = ["CO"]
# STATES = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]
STATES = ["Alabama"]

# This one will only be used in case tables are empty
DEFAULT_START_DATE = datetime(1980, 1, 1)

LAST_SELENIUM_DATE = datetime.now()

CACHE_FILE = '../../cache.pkl'


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


def get_last_update_date():
    last_update = None
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
            last_update = cache.get('last_update')
    return last_update


def get_current_site_update():
    url = 'https://aqs.epa.gov/aqsweb/airdata/download_files.html'

    # Fetch the webpage
    response = requests.get(url)
    if response.status_code == 200:
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Find all tables in the HTML
            tables = soup.find_all('table', class_='tablebord')
            if tables:
                # Get the first table
                table = tables[2]
                # Find all rows in the table
                rows = table.find_all('tr')
                # Skip the header row
                row = rows[1]
                # Get the maximum year
                max_date = row.find_all('td')[1].text.split('As of ')[-1]
                return max_date
        except Exception:
            return None


def update_cache():
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump({'last_update': get_current_site_update()}, f)


def get_max_year():
    url = 'https://aqs.epa.gov/aqsweb/airdata/download_files.html'

    # Fetch the webpage
    response = requests.get(url)
    if response.status_code == 200:
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Find all tables in the HTML
            tables = soup.find_all('table', class_='tablebord')
            if tables:
                # Get the first table
                table = tables[2]
                # Find all rows in the table
                rows = table.find_all('tr')
                # Skip the header row
                row = rows[1]
                # Get the maximum year
                max_year = row.find('em').text
                return int(max_year)
        except Exception as e:
            print(f"Error: {e}")
        return 2023


LAST_ZIP_DATE = datetime(get_max_year(), 1, 1)

if __name__ == '__main__':
    print(get_current_site_update())