import argparse

from config import config
from database.create import create_database
from database.validate import check_database_existence
from database.insert import insert_postgres_table_from_df
from tables.cache import get_gtfs_tables
from files import get_gtfs_text_files
import pandas as pd

# Default config file
DEFAULT_CONFIG_FILE = "config/config.yaml"

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process GTFS data and insert into database.")
parser.add_argument("--config", help="Path to the config file", default=DEFAULT_CONFIG_FILE)
args = parser.parse_args()

# Database connection parameters
DB_NAME = config.get_database()
DB_USER = config.get_user()
DB_PASSWORD = config.get_password()
DB_HOST = config.get_host()
DB_PORT = config.get_port()

# Files
ROUTE_FILE = 'public/routes.txt'
STOP_FILE = 'public/stops.txt'
STOP_TIMES_FILE = 'public/stop_times.txt'
TRIPS_FILE = 'public/trips.txt'

# Tables
LINE_TABLE_NAME = "line_stops"
STOP_DETAILS_NAME = "stop_details"


def insert_tables(tables: list[tuple[str, pd.DataFrame]]) -> None:
    for table_name, df in tables:
        # Insert line stops dataframe into postgres db
        insert_postgres_table_from_df(
            df,
            table_name,
            DB_NAME,
            DB_USER,
            DB_PASSWORD,
            DB_HOST,
            DB_PORT
        )


def run_script():
    try:
        # Download gtfs files if necessary
        get_gtfs_text_files()

        print("\033[92m")
        print("Connecting to database...")
        print(f"Database name: {DB_NAME}")
        print(f"Username: {DB_USER}")
        print("Password: ********")
        print(f"Host: {DB_HOST}")
        print(f"Port: {DB_PORT}")
        print("\033[0m")

        if not check_database_existence(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT):
            # Drops if exist
            create_database(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

        # Create two pandas dataframes from gtfs files
        line_stop_table, stop_details_table = get_gtfs_tables(
            ROUTE_FILE,
            STOP_FILE,
            STOP_TIMES_FILE,
            TRIPS_FILE,
            use_cache=True
        )

        # Insert each table into postgres db
        insert_tables([(LINE_TABLE_NAME, line_stop_table), (STOP_DETAILS_NAME, stop_details_table)])

    except Exception as e:
        print(e.with_traceback())


if __name__ == "__main__":
    run_script()
