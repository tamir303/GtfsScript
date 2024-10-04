import logging
import sys

import pandas as pd
import functools
from typing import Tuple
from tqdm import tqdm


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


@functools.lru_cache(maxsize=None)
def __load_gtfs_data(
        routes_file: str,
        stops_file: str,
        stop_times_file: str,
        trips_file: str,
        agency_file: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads GTFS data from CSV files.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing four pandas DataFrames
        representing routes, trips, stop times, and stops data.
    """
    with tqdm(total=5, desc="Loading GTFS Text files") as pbar:
        routes = pd.read_csv(routes_file)
        pbar.update(1)
        logging.info("Loaded {} routes.txt".format(routes.shape[0]))

        stop_times = pd.read_csv(stop_times_file)
        pbar.update(1)
        logging.info("Loaded {} stop_times.txt".format(stop_times.shape[0]))

        stops = pd.read_csv(stops_file)
        pbar.update(1)
        logging.info("Loaded {} stops.txt".format(stops.shape[0]))

        trips = pd.read_csv(trips_file)
        pbar.update(1)
        logging.info("Loaded {} trips.txt".format(trips.shape[0]))

        agencies = pd.read_csv(agency_file)
        pbar.update(1)
        logging.info("Loaded {} agency.txt".format(agencies.shape[0]))

    return routes, stop_times, stops, trips, agencies


def __create_bus_tables(
        routes_df: pd.DataFrame = None,
        stop_times_df: pd.DataFrame = None,
        stops_df: pd.DataFrame = None,
        trips_df: pd.DataFrame = None,
        agency_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Creates a table with line number, stop name, stop order, latitude, longitude, and agency name.

    Returns:
        pd.DataFrame: A DataFrame containing the line number, stop name, stop order, latitude, longitude, and agency name.
    """
    with tqdm(total=7, desc="Processing GTFS Data") as pbar:
        logging.info("Creating unified GTFS dataframe...")

        # Merge routes with agencies to get agency information
        routes_agency_df = pd.merge(routes_df, agency_df, on='agency_id', how='inner')
        pbar.update(1)

        # Merge trips with routes_agency to get route and agency information
        trips_routes_df = pd.merge(trips_df, routes_agency_df, on='route_id', how='inner')
        pbar.update(1)

        # Merge stop_times with trips_routes to get route, stop times, and agency information
        stop_times_trips_routes_df = pd.merge(stop_times_df, trips_routes_df, on='trip_id', how='inner')
        pbar.update(1)

        # Merge the above result with stops to get the stop details
        full_df = pd.merge(stop_times_trips_routes_df, stops_df, on='stop_id', how='inner')
        pbar.update(1)

        # Select and rename the relevant columns including the agency name
        line_stop_df = full_df[['route_short_name', 'stop_name', 'stop_sequence', 'stop_lat', 'stop_lon', 'agency_name']]
        line_stop_df.rename(columns={
            'route_short_name': 'line_number',
            'stop_name': 'stop_name',
            'stop_sequence': 'stop_order',
            'stop_lat': 'lat',
            'stop_lon': 'lng',
            'agency_name': 'agency_name'
        }, inplace=True)
        pbar.update(1)

        # Remove duplicates and null
        line_stop_df.dropna(inplace=True)
        pbar.update(1)

        line_stop_df.drop_duplicates(inplace=True)
        pbar.update(1)

    return line_stop_df


@functools.lru_cache(maxsize=None)
def create_gtfs_tables(
        routes_file: str,
        stops_file: str,
        stop_times_file: str,
        trips_file: str,
        agency_file: str,
) -> pd.DataFrame:
    """
    Creates GTFS tables line_stop_table and stop_details_table.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two pandas DataFrames.
        The first one is line_stop_table and the second one is stop_details_table
    """
    routes, stop_times, stops, trips, agencies = __load_gtfs_data(routes_file, stops_file, stop_times_file, trips_file, agency_file)
    return __create_bus_tables(
        routes_df=routes,
        stop_times_df=stop_times,
        stops_df=stops,
        trips_df=trips,
        agency_df=agencies
    )
