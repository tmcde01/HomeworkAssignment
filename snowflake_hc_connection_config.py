import os
import sys
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path='admin/healthcare_data_exports.env')
USERNAME = os.getenv("SF_USER")
PASSWORD = os.getenv("SF_PASSWORD")
ACCOUNT = os.getenv("SF_ACCOUNT")


def connect_to_snowflake():
    try:
        sf_connection = snowflake.connector.connect(user=USERNAME,
                                                    password=PASSWORD,
                                                    account=ACCOUNT,
                                                    authenticator='snowflake')
        print("\t--Snowflake connection successful...")
        return sf_connection
    except Exception as e:
        print(f"\t!!!Error connecting to Snowflake: {e}")
        return None


def disconnect_from_snowflake(sf_connection):
    try:
        sf_connection.close()
        print("\t...Snowflake connection closed.")
    except Exception as e:
        print(f"\t!!!Error closing the Snowflake connection: {e}.  sys.exit(1) will be invoked")
        sys.exit(1)


"/etc/pki/tls/certs/ca-bundle.crt"