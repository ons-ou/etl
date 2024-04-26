import os
import pickle

from bs4 import BeautifulSoup
import requests

CACHE_FILE = '../../cache.pkl'

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