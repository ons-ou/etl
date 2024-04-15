import os


class BaseDownloader:
    def __init__(self, directory):
        self.download_folder = directory
        if not (os.path.exists(directory)):
            # If it doesn't exist, create the directory
            os.makedirs(directory)

    def download_data(self, element, year, state):
        raise NotImplementedError("Subclass must implement abstract method")

    def cleanup(self):
        raise NotImplementedError("Subclass must implement abstract method")
