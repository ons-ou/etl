import logging

from scripts.etl.ZipWorflow import ZipWorkflow

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# TODO: Run this to fill database tables
if __name__ == '__main__':
    ZipWorkflow().workflow_thread()
