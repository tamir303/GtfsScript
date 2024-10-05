import logging
import sys
import dask.dataframe as dd
from typing import Tuple
from tqdm import tqdm
import functools

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
) -> Tuple[dd.DataFrame, dd.DataFrame, dd.DataFrame, dd.DataFrame, dd.DataFrame]:
    """
    Loads GTFS data from CSV files as Dask DataFrames.

    Returns:
        tuple[dd.DataFrame, dd.DataFrame, dd.DataFrame, dd.DataFrame]: A tuple containing five Dask DataFrames
        representing routes, trips, stop times, stops, and agencies data.
    """

    # Specify the dtypes for columns that may have mismatched types
    dtype_dict = {
        'route_color': 'object',
        'shape_dist_traveled': 'float64',
        'zone_id': 'float64',
        'shape_id': 'float64'
    }

    with tqdm(total=5, desc="Loading GTFS Text files") as pbar:
        routes = dd.read_csv(routes_file, dtype=dtype_dict, blocksize=25e6)
        pbar.update(1)
        logging.info("\033[36mLoaded {} routes.txt\033[0m".format(routes.shape[0].compute()))

        stop_times = dd.read_csv(stop_times_file, dtype=dtype_dict, blocksize=25e6)
        pbar.update(1)
        logging.info("\033[36mLoaded {} stop_times.txt\033[0m".format(stop_times.shape[0].compute()))

        stops = dd.read_csv(stops_file, dtype=dtype_dict, blocksize=25e6)
        pbar.update(1)
        logging.info("\033[36mLoaded {} stops.txt\033[0m".format(stops.shape[0].compute()))

        trips = dd.read_csv(trips_file, dtype=dtype_dict, blocksize=25e6)
        pbar.update(1)
        logging.info("\033[36mLoaded {} trips.txt\033[0m".format(trips.shape[0].compute()))

        agencies = dd.read_csv(agency_file, dtype=dtype_dict, blocksize=25e6)
        pbar.update(1)
        logging.info("\033[36mLoaded {} agency.txt\033[0m".format(agencies.shape[0].compute()))

    return routes, stop_times, stops, trips, agencies


def __create_bus_tables(
        routes_df: dd.DataFrame,
        stop_times_df: dd.DataFrame,
        stops_df: dd.DataFrame,
        trips_df: dd.DataFrame,
        agency_df: dd.DataFrame
) -> dd.DataFrame:
    """
    Creates a table with line number, stop name, stop order, latitude, longitude, and agency name.

    Returns:
        dd.DataFrame: A Dask DataFrame containing the line number, stop name, stop order, latitude, longitude, and agency name.
    """
    with tqdm(total=6, desc="Processing GTFS Data") as pbar:
        logging.info("\033[32mCreating unified GTFS dataframe...\033[0m")

        # Merge routes with agencies to get agency information
        routes_agency_df = routes_df.merge(agency_df, on='agency_id', how='inner')
        pbar.update(1)

        # Merge trips with routes_agency to get route and agency information
        trips_routes_df = trips_df.merge(routes_agency_df, on='route_id', how='inner')
        pbar.update(1)

        # Merge stop_times with trips_routes to get route, stop times, and agency information
        stop_times_trips_routes_df = stop_times_df.merge(trips_routes_df, on='trip_id', how='inner')
        pbar.update(1)

        # Merge the above result with stops to get the stop details
        full_df = stop_times_trips_routes_df.merge(stops_df, on='stop_id', how='inner')
        pbar.update(1)

        # Select and rename the relevant columns including the agency name
        line_stop_df = full_df[['route_short_name', 'stop_name', 'stop_sequence', 'stop_lat', 'stop_lon', 'agency_name']]
        line_stop_df.columns = ['line_number', 'stop_name', 'stop_order', 'lat', 'lng', 'agency_name']
        pbar.update(1)

        # Remove duplicates and nulls
        line_stop_df = line_stop_df.dropna().drop_duplicates()
        pbar.update(1)

    return line_stop_df


@functools.lru_cache(maxsize=None)
def create_gtfs_tables(
        routes_file: str,
        stops_file: str,
        stop_times_file: str,
        trips_file: str,
        agency_file: str,
) -> dd.DataFrame:
    """
    Creates GTFS tables line_stop_table and stop_details_table.

    Returns:
        dd.DataFrame: A Dask DataFrame containing line-stop information.
    """
    routes, stop_times, stops, trips, agencies = __load_gtfs_data(routes_file, stops_file, stop_times_file, trips_file, agency_file)
    return __create_bus_tables(
        routes_df=routes,
        stop_times_df=stop_times,
        stops_df=stops,
        trips_df=trips,
        agency_df=agencies
    )
