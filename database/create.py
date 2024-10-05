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

        # Check if the database exists
        try:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;"), [db_name])
            exists = cursor.fetchone() is not None

            if exists:
                # If it exists, drop the database
                cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
                logging.info(f"\033[32mDatabase '{db_name}' dropped successfully!\033[0m")

        except OperationalError as e:
            if "does not exist" in str(e):
                logging.info(f"\033[32mDatabase '{db_name}' does not exist; ready to create.\033[0m")
            else:
                logging.error(f"Operational error: {e}")
                raise e

        # Create the database
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        logging.info(f"Database '{db_name}' created successfully!")

        # Grant privileges (optional)
        cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {};").format(
            sql.Identifier(db_name),
            sql.Identifier(user)
        ))

        logging.info(f"\033[32mPrivileges granted on database '{db_name}' to user '{user}'.\033[0m")

    except OperationalError as e:
        logging.error(f"Error: {e}")
        raise e

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
