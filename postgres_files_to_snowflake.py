import os
import time
import schedule
import subprocess
from dotenv import load_dotenv
from collections import namedtuple
from datetime import datetime, timezone
from postgres_hc_connection_config import connect_to_postgres, disconnect_from_postgres

load_dotenv(dotenv_path='admin/healthcare_data_exports.env')

def gen_connection_args():
    utc_timestamp = datetime.now(timezone.utc)
    utc_date = f"{utc_timestamp.strftime('%Y%m%d')}_UTC"
    utc_time = f"{utc_timestamp.strftime('%Y%m%d%H%M%S')}_UTC"
    rsa_key_password = os.getenv("PRIVATE_KEY_PASSWORD")

    source_args_obj = namedtuple("source_args_obj", ["db",
                                                                         "schema",
                                                                         "table_key",
                                                                         "output_directory",
                                                                         "user",
                                                                         "user_password",
                                                                         "host",
                                                                         "port"])

    source_args = source_args_obj(db = 'healthcare_project',
                                  schema = 'general_hospital',
                                  table_key = 'master_patient_id',
                                  output_directory = f"/home/tommy/PycharmProjects/Healthcare_Data_Exports/file_transfer_directory/healthcare_table_files_{utc_date}",
                                  user = "tommy",
                                  user_password = os.getenv("PG_PASSWORD"),
                                  host = "127.0.0.1",
                                  port = 5432)

    target_args_obj = namedtuple("target_args_obj", ["wh",
                                                                         "db",
                                                                         "schema",
                                                                         "stage"])

    target_args = target_args_obj(wh = 'COMPUTE_WH',
                                  db = 'HEALTHCARE_PROJECT',
                                  schema='GENERAL_HOSPITAL_PATIENTS',
                                  stage = 'INBOUND_HEALTHCARE_FILES')

    return utc_time, rsa_key_password, source_args, target_args


def create_file_directory(source_args):
    print(f"\t--Creating file directory at: {source_args.output_directory}...")
    continue_process = False
    try:
        if not os.path.exists(source_args.output_directory):
            os.makedirs(source_args.output_directory)
            print(f"\t\t...Directory created at {source_args.output_directory}. Continuing process.")
            continue_process = True
        else:
            print(f"\t\t!!! Error creating directory:  Directory already exists at {source_args.output_directory}.")
            print("\t\t\t--Exiting process to prevent unintended destruction of existing files.")
        return continue_process

    except Exception as e:
        print(f"\t!!! General error occurred in function create_file_directory(): {e}")
        return continue_process


def open_connections():
    pg_connection = connect_to_postgres()
    return pg_connection


def create_files_from_pg_tables(pg_connection, utc_time, source_args):
    print(f"\t--Executing PostgreSQL queries...")
    try:
        cursor = pg_connection.cursor()
        set_search_path = f"SET SEARCH_PATH = {source_args.schema};"
        cursor.execute(set_search_path)

        get_patient_tables = f"""
                              SELECT 
                                 TABLE_NAME  
                              FROM 
                                 INFORMATION_SCHEMA.COLUMNS
                              WHERE 
                                 TABLE_SCHEMA = '{source_args.schema}'
                              AND
                                 COLUMN_NAME = 'master_patient_id'
                              ORDER BY
                                 TABLE_NAME;
                              """
        cursor.execute(get_patient_tables)

        # Fetch all values from the selected column
        patient_tables = [row[0] for row in cursor.fetchall()]  # Extract first element from each row

        print("\t\t...PostgreSQL queries executed successfully.")
        print(f"\t--Generating PostgreSQL source table files...")
        for patient_table in patient_tables:
            try:
                env = os.environ.copy()
                env["PGPASSWORD"] = source_args.user_password
                output_file = f"{source_args.output_directory}/{patient_table}_{utc_time}.csv"
                copy_command = (f"\\copy (SELECT * FROM {source_args.schema}.{patient_table} "
                                f"ORDER BY {source_args.table_key}) "
                                f"TO '{output_file}' WITH (FORMAT csv, HEADER true, DELIMITER ',');")
                subprocess.run(["psql", "-d", str(source_args.db), "-U", str(source_args.user),
                                     "-h", str(source_args.host), "-p", str(source_args.port), "-c", copy_command],
                                     env=env, check=True, text=True)
                print(f"\t\t...Exported table: {patient_table} to {output_file}")

            except subprocess.CalledProcessError as e:
                print(f"\t\t!!! Error exporting {patient_table}: {e}.  Stopping iteration and moving to next file")
                continue

        print("\t...PostgreSQL source table file generation complete.")
        return patient_tables

    except Exception as e:
        print(f"\t!!! General error occurred in function create_files_from_pg_tables(): {e}. PostgreSQL and Snowflake Connectors will be closed")
        close_connections(pg_connection)


def move_files_to_sf(source_args, target_args, utc_time, rsa_key_password, patient_tables):
    print(f"\t--Moving PostgreSQL source table files from {source_args.output_directory} to Snowflake internal stage at: {target_args.stage}...")
    try:
        env = os.environ.copy()
        env["PRIVATE_KEY_PASSPHRASE"] = rsa_key_password
        # env["REQUESTS_CA_BUNDLE"] = "/etc/pki/tls/certs/ca-bundle.crt"
        print('\t\t--Setting database, schema, and warehouse connection parameters...')
        set_workspace = (f"USE DATABASE {target_args.db}; "
                         f"USE SCHEMA {target_args.schema}; "
                         f"USE WAREHOUSE {target_args.wh};")

        subprocess.run(["snow", "sql", "-c", "healthcare_project", "-q", set_workspace], env=env, check=True, text=True)
        print('\t\t...Database, schema, and warehouse connection parameters successfully set.')

        print(f"\t\t--Creating internal stage if not exists at {target_args.db}.{target_args.schema}.{target_args.stage}...")
        create_stage_command = f"""
                                CREATE STAGE IF NOT EXISTS {target_args.db}.{target_args.schema}.{target_args.stage}
                                    DIRECTORY = (ENABLE = TRUE
                                                 REFRESH_ON_CREATE = TRUE);
                                """
        subprocess.run(["snow", "sql",  "-c",  "healthcare_project", "-q", create_stage_command], env=env, check=True, text=True)
        print("\t\t...Internal stage create command successful.")

        for patient_table in patient_tables:
            try:
                local_file = f"{source_args.output_directory}/{patient_table}_{utc_time}.csv"
                stage_location = f"@{target_args.db}.{target_args.schema}.{target_args.stage}"
                subprocess.run(["snow", "stage", "copy", local_file, stage_location,"-c", "healthcare_project", "--auto-compress"],
                                     env=env, check=True, text=True)
                print(f'\t\t--File for table: {patient_table}_{utc_time} successfully copied into Snowflake internal stage at: {target_args.stage}')

            except Exception as e:
                print(f"\t\t!!!Error executing copy command for table: {patient_table}.  Exception message: {e}")
                print("\t\t\t--Stopping current iteration and moving to next file")
                continue

    except Exception as e:
        print(f"\t!!!General error occurred in function move_files_to_sf(): {e}.")
        return None


def close_connections(pg_connection):
    disconnect_from_postgres(pg_connection)


def run_functions():
    print(f'Starting file data transfer process from PostgreSQL to Snowflake for patient tables...')
    utc_time, rsa_key_password, source_args, target_args = gen_connection_args()
    continue_process = create_file_directory(source_args)
    if continue_process:
        pg_connection = open_connections()
        if not pg_connection:
            print("...PostgreSQL connection failed. Stopping current procedure run.")
        else:
            patient_tables = create_files_from_pg_tables(pg_connection, utc_time, source_args)
            if not patient_tables:
                close_connections(pg_connection)
                print("...No patient tables exported.  Stopping current procedure run.")
            else:
                move_files_to_sf(source_args, target_args, utc_time, rsa_key_password, patient_tables)
                close_connections(pg_connection)
                print(f'...file data transfer from PostgreSQL to Snowflake for patient tables complete.')
                print('End file data transfer process.')
    else:
        print(f'...stopping file data transfer from PostgreSQL to Snowflake for patient tables; process not successful.')

if __name__ == '__main__':
    run_functions()
    # schedule.every().day.at("08:00").do(run_functions)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(5)

