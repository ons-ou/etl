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
    def transform_data(path):
    """
    Transforms and cleans a given dataset.

    Args:
        path (str): Path to the CSV file containing the data.

    Returns:
        pandas.DataFrame: The cleaned and transformed DataFrame.
    """

    df = pd.read_csv(path)

    # Drop columns with constant values
    df = df.loc[:, (df.nunique() != 1)]  # Select columns with variation

    # Filter out data related to Mexico and Puerto Rico (assuming country column exists)
    df = df[df['country'].isin(['US'])]  # Filter for US only

    # Clean coordinate column (assuming it's a string representing a tuple)
    df['coordinate'] = df['coordinate'].apply(lambda x: eval(x))  # Convert string to tuple

    # Filter rows within US boundaries (assuming specific latitude/longitude ranges)
    us_lat_min, us_lat_max = 19.50139, 64.85694
    us_lon_min, us_lon_max = -161.75583, -68.01197
    df = df[(df['coordinate'][0] >= us_lat_min) & (df['coordinate'][0] <= us_lat_max) &
             (df['coordinate'][1] >= us_lon_min) & (df['coordinate'][1] <= us_lon_max)]

    # Remove rows with missing air quality index values
    df = df.dropna(subset=['air_quality_index'])



    # TODO: combine each month for each coordinate for easier visualizations
    return df

    


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
