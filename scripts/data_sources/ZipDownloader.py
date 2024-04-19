import os
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
        response = requests.get(url)
        with open(zip_file_path, "wb") as file:
            file.write(response.content)

        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(self.download_folder)

        os.remove(zip_file_path)

        return csv_file_path

    def cleanup(self):
        os.remove(self.download_folder)
