import logging
import sys

import psycopg2
from psycopg2 import OperationalError, sql

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
            dbname="postgres",  # Connect to the postgres maintenance database
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True  # Enable autocommit to execute commands like CREATE/DROP
        cursor = conn.cursor()

        # Check if the database exists using a parameterized query
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), [db_name])

        # Drop the database if it exists
        cursor.execute(sql.SQL(f"DROP DATABASE IF EXISTS {db_name}"))
        logging.info(f"Database '{db_name}' dropped successfully!")

        # Create the database
        cursor.execute(sql.SQL(f"CREATE DATABASE {db_name}").format(sql.Identifier(db_name)))

        # Grant privileges (optional)
        cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
            sql.Identifier(db_name),
            sql.Identifier(user)
        ))

        logging.info(f"Database '{db_name}' created successfully and privileges granted!")

        # Close cursor and connection
        cursor.close()
        conn.close()

    except OperationalError as e:
        logging.error(f"Error: {e}")
        raise e

