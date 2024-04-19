import logging

from scripts.etl.SeleniumWorkflow import SeleniumWorkflow
from scripts.etl.ZipWorflow import ZipWorkflow

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

if __name__ == '__main__':
    #ZipWorkflow().workflow_thread()
    SeleniumWorkflow().workflow_thread()
