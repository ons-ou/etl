import logging
import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor

from scripts.etl.etl import workflow_init, extract_data, load_data, transform_data


def transform_data_and_load(element, path, max_date):
    if path is not None:
        logging.info(f"Transforming data for {element} from {path}")
        aqi_df, co_df = transform_data(path, max_date)
        logging.info(f"Loading data for {element} from {path}")

        load_data(aqi_df, co_df, element)
        parent_dir = os.path.dirname(path)
        shutil.rmtree(parent_dir)


def workflow_thread():
    with ThreadPoolExecutor() as executor:
        futures = []
        for element, max_date in workflow_init():
            if max_date is not None:
                logging.info(f"Starting Extraction for {element} after {max_date}")
                for path in extract_data(element, max_date):
                    logging.info(f"Extracted data to {path}")
                    future = executor.submit(transform_data_and_load, element, path, max_date)
                    futures.append(future)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

if __name__ == '__main__':
    thread = threading.Thread(target=workflow_thread)
    thread.start()
    thread.join()
