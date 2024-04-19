import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from scripts.data_sources.BaseDownloader import BaseDownloader


class SeleniumDownloader(BaseDownloader):
    def __init__(self, directory):
        super().__init__(directory)
        self.initial_files = set(os.listdir(self.download_folder))

        options = Options()
        prefs = {
            "download.default_directory": self.download_folder,
            "download.prompt_for_download": False,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        # options.add_argument("--headless")

        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(30)

    def download_data(self, element, year, state, cbsa=-1, county=-1, site=-1):
        """
          A faster method for downloading data by sending a request instead of manually filling the form and waiting for each input.
          However, couldn't send a direct request for download because the download button triggers a Google Analytics event
          rather than directly initiating the download.
        """
        name = f"{element}_{state}_{year}"
        csv_file_path = os.path.join(self.download_folder, f"{name}.csv")

        if os.path.exists(csv_file_path):
            return csv_file_path

        try:
            # Construct the URL with the form data
            url = f"https://www3.epa.gov/cgi-bin/broker?_service=data&_debug=0&_program=dataprog.ad_data_daily_airnow.sas&poll={element}&year={year}&state={state}&cbsa={cbsa}&county={county}&site={site}"

            # Open the URL
            self.driver.get(url)

            # Click on the download button
            self.driver.find_element(By.LINK_TEXT, "Download CSV (spreadsheet)").click()

            # Wait for the download to complete
            return self.wait_for_new_file()
        except Exception as e:
            print("An error occurred:", e)
            return None
        finally:
            self.cleanup()

    def wait_for_new_file(self):
        while True:
            current_files = set(os.listdir(self.download_folder))
            new_files = current_files - self.initial_files
            try:
                if new_files:
                    file = new_files.pop()
                    if file.endswith(".csv"):
                        return file
            except KeyError:
                pass
            time.sleep(1)

    def cleanup(self):
        self.driver.quit()
