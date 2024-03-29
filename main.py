import logging
import multiprocessing

from etl import extract_data, transform_data, merge_data

logging.basicConfig(level=logging.INFO)


def etl_process(element, state, year):
    # Perform ETL process for the given element, state, and year
    logging.info(f"Starting Extraction process for element: {element}, state: {state}, year: {year}")
    path = extract_data(element, state, year)
    logging.info(f"Completed Extraction process for element: {element}, state: {state}, year: {year}")
    logging.info(f"Starting Transformation process for element: {element}, state: {state}, year: {year}")
    transform_data(path)
    logging.info(f"Completed Transformation process for element: {element}, state: {state}, year: {year}")
    logging.info(f"Starting Merging process for element: {element}, state: {state}, year: {year}")
    merge_data(element, path)
    logging.info(f"Completed Merging process for element: {element}, state: {state}, year: {year}")


def parallel_etl(elements, states, years):
    # Create a pool of processes
    with multiprocessing.Pool() as pool:
        # Map the ETL function to all combinations of years
        pool.starmap(etl_process,
                     [(element, state, year) for element in elements for state in states for year in years])


if __name__ == "__main__":
    # TODO: final result should only add new data not already in database
    elements = ["CO"]  # Add all elements
    states = ["Indiana"]  # Add all states
    years = range(2020, 2023)  # From 1980 to current year

    parallel_etl(elements, states, years)
