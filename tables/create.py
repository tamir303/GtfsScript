import pandas as pd
import functools

from pandas import DataFrame


def __load_gtfs_data(
        routes_file: str,
        stops_file: str,
        stop_times_file: str,
        trips_file: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads GTFS data from CSV files.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing four pandas DataFrames
        representing routes, trips, stop times, and stops data.
    """
    routes = pd.read_csv(routes_file)
    stop_times = pd.read_csv(stop_times_file)
    stops = pd.read_csv(stops_file)
    trips = pd.read_csv(trips_file)

    return routes, stop_times, stops, trips


def __create_bus_tables(
        routes_df: pd.DataFrame = None,
        stop_times_df: pd.DataFrame = None,
        stops_df: pd.DataFrame = None,
        trips_df: pd.DataFrame = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Creates bus data tables from CSV files

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing two pandas DataFrames
        representing gtfs tables, one for routes and one for stops
    """

    # Merge routes and trips on route_id
    routes_trips_df = pd.merge(routes_df, trips_df, on='route_id', how='inner')

    # Merge routes_trips with stop_times on trip_id
    bus_trips_df = pd.merge(routes_trips_df, stop_times_df, on='trip_id', how='inner')

    # Extract unique trip_id and stop_id for bus routes
    line_stop_table = bus_trips_df[['route_short_name', 'stop_id']].drop_duplicates()

    # Rename route_short_name to route_line
    line_stop_table.rename(columns={'route_short_name': 'route_line'}, inplace=True)

    # Sort by route_line in ascending order
    line_stop_table.sort_values(by='route_line', inplace=True)

    # Merge stop_times and stops to get stop details
    stop_details_df = pd.merge(bus_trips_df[['trip_id', 'stop_id', 'arrival_time']], stops_df, on='stop_id', how='inner')

    # Drop unnecessary columns
    stop_details_df = stop_details_df[['stop_id', 'arrival_time', 'stop_lat', 'stop_lon']].drop_duplicates()

    # Group by stop_id to get unique stop_id with the earliest arrival_time
    stop_details_df = stop_details_df.groupby('stop_id').first().reset_index()

    return line_stop_table, stop_details_df


@functools.lru_cache(maxsize=None)
def create_gtfs_tables(
        routes_file: str,
        stops_file: str,
        stop_times_file: str,
        trips_file: str
) -> tuple[DataFrame, DataFrame]:
    """
    Creates GTFS tables line_stop_table and stop_details_table.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two pandas DataFrames.
        The first one is line_stop_table and the second one is stop_details_table
    """
    routes, stop_times, stops, trips = __load_gtfs_data(routes_file, stops_file, stop_times_file, trips_file)
    return __create_bus_tables(
        routes_df=routes,
        stop_times_df=stop_times,
        stops_df=stops,
        trips_df=trips)
