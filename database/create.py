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


def create_database(
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

        # Check if the database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';")
        exists = cursor.fetchone()

        if exists:
            # Drop the database if it exists
            cursor.execute(f"DROP DATABASE {db_name};")
            print(f"Database '{db_name}' dropped successfully!")

        # Create the database
        cursor.execute(f"CREATE DATABASE {db_name};")

        # Grant privileges (optional)
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {user};")

        logging.info("Database created successfully!")

        # Close cursor and connection
        cursor.close()
        conn.close()

    except OperationalError as e:
        logging.error(f"Error: {e}")
        raise e
