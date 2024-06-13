import logging
import sys

from config import config
from database.create import create_database
from database.validate import check_database_existence
from database.insert import insert_postgres_table_from_df
from tables.cache import get_gtfs_tables
from files import get_gtfs_text_files

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
    logging.info("Starting GTFS data processing and database insertion script.")

    try:
        # Download GTFS files if necessary
        get_gtfs_text_files()

        logging.info("Connecting to database...")
        logging.info(f"Database name: {db_config['name']}")
        logging.info(f"Username: {db_config['user']}")
        logging.info("Password: ********")
        logging.info(f"Host: {db_config['host']}")
        logging.info(f"Port: {db_config['port']}")

        if not check_database_existence(db_config["name"], db_config["user"], db_config["password"], db_config["host"], db_config["port"]):
            create_database(db_config["name"], db_config["user"], db_config["password"], db_config["host"], db_config["port"])

        # Create a DataFrame from GTFS files
        line_stop_table = get_gtfs_tables(
            'public/routes.txt',
            'public/stops.txt',
            'public/stop_times.txt',
            'public/trips.txt',
            use_cache=True
        )

        # Insert DataFrame into PostgreSQL database
        insert_postgres_table_from_df(
            line_stop_table,
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
