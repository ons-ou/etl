import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from utils import create_directory

URL = "https://www.epa.gov/outdoor-air-quality-data/download-daily-data"


class EPAAirQualityDownloader:
    def __init__(self, directory):
        self.download_folder = directory
        create_directory(self.download_folder)
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

    def download_data(self, element, state, year):
        # TODO: avoid using so many sleep to be able to successfully download
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
