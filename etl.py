import os
import pandas as pd
from EPAAirQualityDownloader import EPAAirQualityDownloader
from utils import delete_directory

current_directory = os.getcwd()


def extract_data(element, state, year):
    name = f"{element}_{state}_{year}"
    directory = os.path.join(current_directory, "daily_data", name)
    # Perform ETL process for the given element, state, and year
    downloader = EPAAirQualityDownloader(directory)
    file = downloader.download_data(element, state, year)
    return os.path.join(directory, file)


def transform_data(path):
    # TODO: same transformations and cleaning as previously done to CO datasets
    # TODO: combine each month for each coordinate for easier visualizations
    pass


def merge_data(element, path):
    # Read the CSV file
    df = pd.read_csv(path)

    # Path to the output file
    output_file = os.path.join(current_directory, "daily_data", f"daily_{element}_dataset.csv")

    # If the output file doesn't exist, create it and write the header
    if not os.path.exists(output_file):
        df.to_csv(output_file, index=False)
    else:
        # Append to the existing file without writing the header
        df.to_csv(output_file, mode='a', header=False, index=False)

    # Delete the directory containing the original file
    dir_path = os.path.dirname(path)
    if os.path.exists(dir_path):
        delete_directory(dir_path)

# TODO: Load data to database of choice. If database already has values in it make sure to only append new data
