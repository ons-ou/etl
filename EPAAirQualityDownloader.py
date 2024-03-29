import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
        """Downloads data from a website, avoiding excessive sleep.

        Args:
            element: The element to select (e.g., "PM2.5").
            state: The state abbreviation (e.g., "CA").
            year: The year to download data for (e.g., 2023).

        Returns:
            The downloaded file path (if successful).
        """

        # Set explicit wait timeouts for better control
        timeout = 10  # Adjust timeout as needed (in seconds)

        self.driver.find_element(By.ID, "poll").click()
        Select(self.driver.find_element(By.ID, "poll")).select_by_visible_text(element)

        # Wait for "poll" element to change (optional, for dynamic content)
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.ID, "year"))
            )
        except TimeoutException:
            print("Warning: Timed out waiting for 'year' element after selecting poll.")

        self.driver.find_element(By.ID, "year").click()
        Select(self.driver.find_element(By.ID, "year")).select_by_visible_text(str(year))

        # Wait for "year" element to change (optional)
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.ID, "state"))
            )
        except TimeoutException:
            print("Warning: Timed out waiting for 'state' element after selecting year.")

        self.driver.find_element(By.ID, "state").click()
        Select(self.driver.find_element(By.ID, "state")).select_by_visible_text(state)

        Select(self.driver.find_element(By.ID, "site")).select_by_value("-1")

        # Implement presence check or URL change for download button
        download_button = WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, "//input[@value='Get Data']"))
        )
        download_button.click()

        # Consider waiting for a download completion indicator (e.g., progress bar)
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.url_to_be("https://your-download-completion-url")  # Replace with actual URL
            )
        except TimeoutException:
            print("Warning: Timed out waiting for download confirmation URL.")

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
