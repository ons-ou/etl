import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from scripts.data_sources.BaseDownloader import BaseDownloader

URL = "https://www.epa.gov/outdoor-air-quality-data/download-daily-data"


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
        options.add_argument("--headless")

        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(30)

        self.driver.get(URL)

    def download_data(self, element, year, state):
        name = f"{element}_{state}_{year}"
        csv_file_path = os.path.join(self.download_folder, f"{name}.csv")

        if os.path.exists(csv_file_path):
            return csv_file_path

        self.driver.find_element(By.ID, "poll").click()
        Select(self.driver.find_element(By.ID, "poll")).select_by_visible_text(element)
        time.sleep(5)
        self.driver.find_element(By.ID, "year").click()
        Select(self.driver.find_element(By.ID, "year")).select_by_visible_text(str(year))
        time.sleep(5)
        self.driver.find_element(By.ID, "state").click()
        Select(self.driver.find_element(By.ID, "state")).select_by_visible_text(state)
        time.sleep(5)
        Select(self.driver.find_element(By.ID, "site")).select_by_value("-1")
        time.sleep(5)
        self.driver.find_element(By.XPATH, "//input[@value='Get Data']").click()
        time.sleep(5)
        self.driver.find_element(By.LINK_TEXT, "Download CSV (spreadsheet)").click()
        return self.wait_for_new_file()

    def download_data_2(self, element, year, state,cbsa=-1,county=-1,site=-1):
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
