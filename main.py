import logging
import sys

from config import config
from database.create import create_database
from database.insert import insert_postgres_table_from_df
from tables.cache import get_gtfs_tables
from files import get_gtfs_text_files
from dask.distributed import Client

# Default config file
DEFAULT_CONFIG_FILE = "config/config.yaml"

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


def get_db_config() -> dict:
    """Retrieve database configuration."""
    return {
        "name": config.get_database(),
        "user": config.get_user(),
        "password": config.get_password(),
        "host": config.get_host(),
        "port": config.get_port()
    }


def main() -> None:
    """Main function to run the GTFS data processing and insertion script."""
    db_config = get_db_config()
    Client(n_workers=4)
    logging.info("\033[32mStarting GTFS data processing and database insertion script.\033[0m")

    try:
        # Download GTFS files if necessary
        get_gtfs_text_files()

        logging.info("\033[32mConnecting to database...\033[0m")
        logging.info(f"\033[34mDatabase name: {db_config['name']}\033[0m")
        logging.info(f"\033[34mUsername: {db_config['user']}\033[0m")
        logging.info("\033[34mPassword: ********\033[0m")
        logging.info(f"\033[34mHost: {db_config['host']}\033[0m")
        logging.info(f"\033[34mPort: {db_config['port']}\033[0m")

        create_database(db_config["name"], db_config["user"], db_config["password"], db_config["host"], db_config["port"])

        # Create a DataFrame from GTFS files
        line_stop_table = get_gtfs_tables(
            'public/routes.txt',
            'public/stops.txt',
            'public/stop_times.txt',
            'public/trips.txt',
            'public/agency.txt',
            use_cache=True
        )

        # Compute the result and trigger actual processing
        logging.info("\033[32mComputing data, convert to pandas...\033[0m")
        persisted_df = line_stop_table.persist()
        result = persisted_df.compute()

        # Insert DataFrame into PostgreSQL database
        insert_postgres_table_from_df(
            result,
            "line_stops",
            db_config["name"],
            db_config["user"],
            db_config["password"],
            db_config["host"],
            db_config["port"]
        )

        logging.info("GTFS data successfully processed and inserted into the database.")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()
