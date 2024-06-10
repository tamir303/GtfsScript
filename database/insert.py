import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from io import StringIO


def insert_postgres_table_from_df(
        df: pd.DataFrame,
        table_name: str,
        db_name: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: str = "5432"):

    global cursor, conn

    try:
        print(f"Adding table {table_name} to database {db_name}...")

        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Create a cursor object
        cursor = conn.cursor()

        # Define a function to map DataFrame dtypes to PostgreSQL column types
        def map_dtype(dtype) -> str:
            if pd.api.types.is_integer_dtype(dtype):
                return 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                return 'FLOAT'
            elif pd.api.types.is_bool_dtype(dtype):
                return 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return 'TIMESTAMP'
            else:
                return 'TEXT'

        # Drop table if exists
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)

        # Create table query
        columns = ", ".join([f'"{col}" {map_dtype(dtype)}' for col, dtype in zip(df.columns, df.dtypes)])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

        # Execute create table query
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")

        # Insert data into the table
        sio = StringIO()
        df.to_csv(sio, sep='\t', header=False, index=False)  # Using tab separator
        sio.seek(0)
        cursor.copy_from(sio, f"{table_name}", sep='\t', null="")
        conn.commit()
        print(f"Data inserted into table '{table_name}' successfully.")

    except OperationalError as e:
        print(f"Error: {e}")
        raise e

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
