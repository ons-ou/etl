import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, wait

from scripts.etl.SeleniumWorkflow import SeleniumWorkflow
from scripts.etl.ZipWorflow import ZipWorkflow
from scripts.utils.etl_utils import ELEMENTS
from scripts.utils.zip_workflow_utils import get_last_update_date, get_current_site_update, update_cache

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


def workflow_thread(source):
    cache = False
    if source == 'DAILY':
        self = SeleniumWorkflow()
    else:
        cache = True
        if get_last_update_date() == get_current_site_update():
            logging.info("Up to date")
            return
        self = ZipWorkflow()

    with ThreadPoolExecutor() as executor:
        futures = []
        for element in ELEMENTS:
            logging.info(f"Starting Workflow for {element}")
            future = executor.submit(self.workflow_init, element)
            futures.append(future)

        for future in as_completed(futures):
            element, max_date = future.result()
            if max_date is not None:
                logging.info(f"Starting Extraction for {element} after {max_date}")
                for path in self.extract_data(element, max_date):
                    logging.info(f"Extracted data to {path}")
                    executor.submit(self.transform_data_and_load, element, path, max_date)

        if cache:
            wait(futures)
            update_cache()


if __name__ == '__main__':
    workflow_thread('COMPILED')
    workflow_thread('DAILY')
