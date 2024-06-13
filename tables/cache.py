from typing import Tuple
from .create import create_gtfs_tables
from .create import pd


def get_gtfs_tables(
        routes_file: str,
        stops_file: str,
        stop_times_file: str,
        trips_file: str,
        use_cache: bool = True
) -> pd.DataFrame:
    if use_cache:
        # Use the cached version
        return create_gtfs_tables(routes_file, stops_file, stop_times_file, trips_file)
    else:
        # Bypass the cache by directly calling the function without using the cached result
        create_gtfs_tables.cache_clear()  # Clear the cache if needed
        return create_gtfs_tables.__wrapped__(routes_file, stops_file, stop_times_file, trips_file)
