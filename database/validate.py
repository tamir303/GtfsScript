import logging
import sys

import psycopg2
from psycopg2 import OperationalError

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


def check_database_existence(
        db_name: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: str = "5432"):
    try:
        # Connect to the default postgres database
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        # Create a cursor object
        cursor = conn.cursor()

        # Query to check if the database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))

        # Fetch one row
        result = cursor.fetchone()

        # Close cursor and connection
        cursor.close()
        conn.close()

        # If result is not None, database exists
        return result is not None

    except OperationalError as e:
        logging.error(f"Error: {e}")
        raise e

