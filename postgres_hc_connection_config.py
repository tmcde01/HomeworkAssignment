import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path='admin/healthcare_data_exports.env')
HOST = os.getenv("PG_HOST")
DATABASE = os.getenv("PG_DATABASE")
USERNAME = os.getenv("PG_USER")
PASSWORD = os.getenv("PG_PASSWORD")
PORT = os.getenv("PG_PORT")


def connect_to_postgres():
    try:
        pg_connection = psycopg2.connect(host=HOST,
                                         database=DATABASE,
                                         user=USERNAME,
                                         password=PASSWORD,
                                         port=PORT)
        print("\t--PostgreSQL connection successful...")
        return pg_connection
    except psycopg2.Error as e:
        print(f"\t!!!Error connecting to PostgreSQL: {e}")
        return None


def disconnect_from_postgres(pg_connection):
    try:
        if pg_connection is None:
            print("No PostgreSQL connection to close")
        else:
            pg_connection.close()
            print("\t...PostgreSQL connection closed.")
    except psycopg2.Error as e:
        print(f"\t!!!Error closing the PostgreSQL connection: {e}.  sys.exit(1) will be invoked")
        sys.exit(1)



