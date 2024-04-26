import os
import time

import requests
import zipfile

from scripts.data_sources.BaseDownloader import BaseDownloader


class ZipDownloader(BaseDownloader):
    def __init__(self, download_folder):
        super().__init__(download_folder)

    def download_data(self, element, year, state=None):
        name = f"daily_{element}_{year}"
        csv_file_path = os.path.join(self.download_folder, f"{name}.csv")

        if os.path.exists(csv_file_path):
            return csv_file_path

        zip_file_path = os.path.join(self.download_folder, f"{name}.zip")
        url = f"https://aqs.epa.gov/aqsweb/airdata/{name}.zip"

        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = requests.get(url, timeout=30)  # Increase timeout as needed
                response.raise_for_status()  # Raise an exception for HTTP errors
                with open(zip_file_path, "wb") as file:
                    file.write(response.content)

                with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                    zip_ref.extractall(self.download_folder)

                os.remove(zip_file_path)

                return csv_file_path

            except (requests.exceptions.ChunkedEncodingError, requests.exceptions.RequestException) as e:
                print(f"An error occurred for {element} {year}: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying ({retry_count}/{max_retries})...")
                    time.sleep(5)  # Wait before retrying

        print("Failed to download data after retrying.")
        return None

    def cleanup(self):
        os.remove(self.download_folder)
