-- SET ROLE
------------------------
use role sysadmin;

-- CREATE AND USE SESSION VARIABLES FOR PORTABILITY
-----------------------------------------------------
set db = 'HOMEWORK_ASSIGNMENT';
set admin_schema = 'ADMIN_SCHEMA';
set raw_schema = 'RAW_BRONZE';
set transform_schema ='TRANSFORM_SILVER';
set target_schema ='TARGET_GOLD';

-- ESTABLISH DB AND SCHEMA OBJECTS
-----------------------------------------------------
-- Notes:  
    -- Use the "create if not exists" convention to prevent destructive acts
    -- Double comment drop object for development work


-- -- drop database identifier($db);
create database if not exists identifier($db);
use database identifier($db);

-- -- drop schema identifier($admin_schema);
create schema if not exists identifier($admin_schema);
-- -- drop schema identifier($landing_schema);
create schema if not exists identifier($raw_schema);
-- -- drop schema identifier($raw_schema);
create schema if not exists identifier($transform_schema);
-- -- drop schema identifier($staged_schema);
create schema if not exists identifier($target_schema);


-- WORK WITHIN ADMIN SCHEMA
-------------------------------------------------------
-- Note:
    -- Use partially qualified references to balance precision with portability / cloneability
    -- Use references:  https://docs.snowflake.com/en/sql-reference/sql/create-file-format

use schema identifier($admin_schema);


-- LOAD FILES MANUALLY THROUGH UI FOR TESTING
-- -- drop stage admin_schema.testing_files;
create stage if not exists admin_schema.testing_files 
directory = (
    enable = true
    refresh_on_create = true
    -- auto_refresh = true  -- Only works for external stages
    );


select * from directory(@admin_schema.testing_files);


-- CREATE A FILE FORMAT FOR THE INFER_SCHEMA FUNCTION
-- -- drop file format admin_schema.infer_schema_loan_monthly;
create file format if not exists admin_schema.infer_schema_pipe_delimited
    type = csv
    encoding = 'UTF8'
    field_delimiter = '|'
    parse_header = true
    field_optionally_enclosed_by = '"'
    replace_invalid_characters = true
    null_if = ('NULL', 'null', 'None', '')
    empty_field_as_null = true
    compression = 'gzip';

-- CREATE A CONTROL TABLE TO BASELINE THE FILE SCHEMA 
create table if not exists admin_schema.loan_monthly_expected_file_schema  as (
    select 
        *
    from table(
            infer_schema(
                location => '@admin_schema.testing_files',
                file_format => 'admin_schema.infer_schema_loan_monthly',
                files => 'LOAN_MONTHLY_202601.csv.gz',
                ignore_case => false
                --max_file_count => <num>
                --max_records_per_file => <num>
                )
            )
      );

select * from admin_schema.loan_monthly_expected_file_schema;

-- USE THE CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR LANDING TABLE


    


-- WORK WITHIN RAW_BRONZE SCHEMA
-------------------------------------------------------
use schema identifier($raw_schema);


-- WORK WITHIN TRANSFORM_SILVER SCHEMA
-------------------------------------------------------
use schema identifier($transform_schema);


-- WORK WITHIN TARGET_GOLD SCHEMA
-------------------------------------------------------
use schema identifier($target_schema);


