from scripts.utils.file_utils import create_directory


class BaseDownloader:
    def __init__(self, directory):
        self.download_folder = directory
        create_directory(self.download_folder)

    def download_data(self, element, year, state):
        raise NotImplementedError("Subclass must implement abstract method")

    def cleanup(self):
        raise NotImplementedError("Subclass must implement abstract method")
