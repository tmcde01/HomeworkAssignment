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


-- CREATE A TEST STAGE AND LOAD FILES MANUALLY THROUGH UI FOR TESTING
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

STOP HERE:  You will need to upload the test file 'LOAN_MONTHLY_202601.csv.gz' to the stage at admin_schema.testing_files  

-- CREATE A CONTROL TABLE TO BASELINE THE FILE SCHEMA 
create table if not exists admin_schema.loan_monthly_expected_file_schema  as (
    select 
        *
    from table(
            infer_schema(
                location => '@admin_schema.testing_files',
                file_format => 'admin_schema.infer_schema_pipe_delimited',
                files => 'LOAN_MONTHLY_202601.csv.gz',
                ignore_case => false
                --max_file_count => <num>
                --max_records_per_file => <num>
                )
            )
      );

select * from admin_schema.loan_monthly_expected_file_schema;
 

-- WORK WITHIN RAW_BRONZE SCHEMA
-------------------------------------------------------
use schema identifier($raw_schema);

-- CREATE THE PERMANENT STAGE FOR FILE DROPS
-- drop stage raw_bronze.daily_files;
create stage if not exists raw_bronze.daily_files 
directory = (
    enable = true
    refresh_on_create = true
    -- auto_refresh = true  -- Only works for external stages
    );

list @raw_bronze.daily_files;
alter stage raw_bronze.daily_files refresh;
select * from directory(@raw_bronze.daily_files);
remove @raw_bronze.daily_files;

-- HERE WE WILL CREATE A STREAM, THOUGH WE WILL NOT USE IT UNTIL WE PUT EVERYTHING TO A PROCEDURE
-- -- drop stream raw_bronze.inbound_loan_monthly_files_stream;
create stream if not exists raw_bronze.inbound_loan_monthly_files_stream
    on stage raw_bronze.daily_files;
-- show streams in schema raw_bronze;

select * from raw_bronze.inbound_loan_monthly_files_stream;
select * from admin_schema.loan_monthly_trigger_reset_temp_table;
    

-- FOR TESTING WE CAN COPY FILES FROM OUR ADMIN SCHEMA TO THE WORKING STAGE IN THE RAW_BRONZE SCHEMA
-- copy files 
--     into @raw_bronze.daily_files 
--     from @admin_schema.testing_files
--     pattern = '^.*\LOAN_MONTHLY_\\d{6}\\.csv\\.gz$';

-- select * from directory(@raw_bronze.daily_files);


-- CREATE A FILE FORMAT FOR FILE INGESTION
-- drop file format raw_bronze.ingest_data_pipe_delimited;
create file format if not exists raw_bronze.ingest_data_pipe_delimited
    type = csv
    encoding = 'UTF8'
    field_delimiter = '|'
    skip_header = 1
    field_optionally_enclosed_by = '"'
    replace_invalid_characters = true
    null_if = ('NULL', 'null', 'None', '')
    empty_field_as_null = true
    trim_space = true
    compression = 'gzip';


-- USE THE CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR LANDING TABLE
-- drop table raw_bronze.raw_loan_monthly;
declare
    db varchar default 'HOMEWORK_ASSIGNMENT';
    admin_schema varchar default 'ADMIN_SCHEMA';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    raw_schema varchar default 'RAW_BRONZE';
    table_name varchar default 'RAW_LOAN_MONTHLY';
    table_type varchar default '';
    schema_evolution boolean default true;
    create_table_head varchar default '';
    create_table_tail varchar default '';

    get_table_ddl varchar;
    table_ddl resultset;
    column_datatype varchar default '';
    column_name varchar default '';
    column_ddl varchar default '';
    create_table_body varchar default '';

    create_table_statement varchar; 

begin

    execute immediate 'use database ' || :db ||';';

    -- INCLUDE OUR METADATA COLUMNS
    create_table_head := 'create ' || :table_type || ' table if not exists ' || :raw_schema || '.' || :table_name || ' (
                                loaded_at timestamp_tz,
                                file_name varchar,
                                file_month varchar,
                                file_unique_key varchar,
                                file_row_number integer,
                                file_record_unique_key varchar, ' || '\n';
    create_table_tail := ') enable_schema_evolution = ' || :schema_evolution ||';';

    get_table_ddl := 'select column_name from ' || :admin_schema || '.' || :control_table || ' order by order_id;';
    table_ddl := (execute immediate get_table_ddl);

    -- HERE WE WILL USE THE ORIGINAL COLUMN NAMES FROM THE FILE WITH DOUBLE-QUOTE NOTATION.
    -- WE'LL ALSO SET ALL DATATYPES TO VARCHAR FOR DURABILITY
    for record in table_ddl do
        column_name := '"' || record.column_name || '"';
        column_datatype := 'VARCHAR';
        column_ddl := :column_name || ' ' || :column_datatype || ',' || '\n';
        create_table_body := :create_table_body || :column_ddl;
    end for;

    create_table_body := rtrim(create_table_body, ',\n');

    create_table_statement := :create_table_head || :create_table_body || :create_table_tail;
    -- return create_table_statement;
    execute immediate create_table_statement;

    return (select * from table(result_scan(last_query_id())));
    
end;


-- WORK WITHIN TRANSFORM_SILVER SCHEMA
-------------------------------------------------------
use schema identifier($transform_schema);

create or replace view transform_silver.vw_loan_monthly_clean as (
-- THIS IS OUR INITIAL TABLE SCAN IN OUR CTE CASCADE.  WE COULD FILTER WITH A WHERE CLAUSE HERE, BUT FOR
-- DEMONSTRATION PURPOSES WE WILL SHOW OUR DATA QUALITY CHECKS
    with get_all_records as (
        select * from raw_bronze.raw_loan_monthly
    ),
    -- select * from get_all_records order by "servicer_name", "loan_id", "updated_at" desc;
    
    -- THIS IS WHERE WE WOULD IDENTIFY AND SEPARATE OUT BAD RECORDS.  WE COULD SAVE THE RESULTS TO A FAILOVER TABLE
    separate_bad_records as (
        select *
        from get_all_records
        where 
            "reporting_month" is null
                or
            "loan_id" is null
                or 
            "loan_id + reporting_month" is null
                or
            to_number("balance", 38, 2) < 0
                or 
            to_number(replace("interest_rate", '%', ''), 38, 2) < 0
                or 
            to_number(replace("interest_rate", '%', ''), 38, 2) > 25
            ),
    -- select * from separate_bad_records order by file_name, "servicer_name", "loan_id", "updated_at" desc;
    
    -- THIS IS WHERE WE WOULD IDENTIFY BAD ROW COUNTS FILES WITH BAD ROW COUNTS, USING STDDEV() AS A PRIMIITIVE MACHINE LEARNING FUNCTION
    check_row_count as (
        select 
            file_unique_key,
            file_name,
            record_count,
            threshold,
            iff(record_count >= threshold,
                   'Pass',
                   'Fail')
              as file_record_count_evaluation
        from 
            (select
                file_unique_key,
                file_name,
                count(*) as record_count,
                ceil(stddev(count(*)) over () * 2.5) as threshold
            from get_all_records
            group by file_name, file_unique_key
            )
        where file_record_count_evaluation = 'Fail'
        ),
    -- select * from check_row_count order by file_name;
    
    -- NOW WE GET TO THE "GOOD" RECORDS, THOUGH WE STILL HAVE WORK TO DO TO GET THE RIGHT ONE . . .
    compile_good_records as (
        select * from get_all_records 
        where 
            file_unique_key not in (select file_unique_key from check_row_count)
                and
            file_unique_key not in (select file_unique_key from separate_bad_records)
        ),
    -- select * from compile_good_records order by "servicer_name", "loan_id", "updated_at" desc;
    
    -- THIS IS THE LAST STEP IN GATHERING THE BEST RECORD FOR EACH LOAN ID FOR OUR VIEW
    -- NOTE A CRITICAL ASSUMPION:
        -- LOADED_AT METADATA WAS CREATED BY US.  LOGICALLLY THE MOST RECENT LOAD WOULD HAVE THE MOST RECENT RECORD.  BUT VENDOR DATA INTEGRITY IS NOT PERFECT SO WE WILL NOT USE IT
        -- FILE_MONTH, WHICH IS ALSO FILE METADATA CREATED FROM THE FILE NAME COULD BE HELPFUL, PERHAPS EVEN THE METADATA FILE_ROW_NUMBER DEPENDING ON VENDOR BEHAVIOR
        -- BUT LOOKING AT THE DATA, THE "updated_at" COLUMN APPEARS TO BE THE MOST SUGGESTIVE OF RECENT VENDOR ACTIVITY.  UNLIKE METADATA, IT IS NOT PROCESS DEPENDENT. 
        -- WE DO NEED TO TALK TO OUR VENDORS TO UNDERSTAND THE BEHAVIOR, BUT HERE WE ASSUME "updated_at" TO BE THE BEST WAY TO ELIMINATE OLD AND DUPLICATE RECORDS AND GET AT 
            -- THE FINAL RECORD WE NEED.  THIS ALSO ALLOWS BATCH INGEST OF MULTIPLE FILE DATES WITHOUT THE PERILS OF INCREMENTAL LOADS.
            -- WE ALSO AVOID HAVING TO DO UPDATE STATEMENTS, WHICH WOULD BE ERROR-PRONE AND RESULT IN A DATA SET THAT INCLUDES OUR OWN DERIVED DATA.  FOR THIS EXERCISE, WE WANT TO AVOID THIS
    select_best_record_for_loan_id as (
        -- DO A LITTLE MAGICAL IN PLACE TRANSFORM ON STATE NAMES
        select * replace (case 
                                when "state" = 'GUAM' then 'GU'
                                when "state" = 'OHIO' then 'OH'
                                when "state" = 'D.C.' then 'DC'
                                when "state" like 'NEW %' then 'N' || substr("state", 5,1)
                                else "state"
                           end as "state")

        from compile_good_records
        qualify row_number() over (partition by "loan_id" order by "updated_at" desc) = 1
    )
    select * from select_best_record_for_loan_id order by "servicer_name", "loan_id", "updated_at" desc
    );

select * from transform_silver.vw_loan_monthly_clean; 



-- WORK WITHIN TARGET_GOLD SCHEMA
-------------------------------------------------------
use schema identifier($target_schema);

-- USE THE CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR TARGET TABLE
-- drop table target_gold.target_loan_monthly;
declare
    db varchar default 'HOMEWORK_ASSIGNMENT';
    admin_schema varchar default 'ADMIN_SCHEMA';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    target_schema varchar default 'TARGET_GOLD';
    table_name varchar default 'TARGET_LOAN_MONTHLY';
    table_type varchar default '';
    schema_evolution boolean default true;
    create_table_head varchar default '';
    create_table_tail varchar default '';

    get_table_ddl varchar;
    table_ddl resultset;
    column_datatype varchar default '';
    column_name varchar default '';
    column_ddl varchar default '';
    create_table_body varchar default '';

    create_table_statement varchar; 

begin

    execute immediate 'use database ' || :db ||';';

    -- INCLUDE OUR METADATA COLUMNS
    create_table_head := 'create ' || :table_type || ' table if not exists ' || :target_schema || '.' || :table_name || ' (
                                loaded_at timestamp_tz,
                                file_name varchar,
                                file_month varchar,
                                file_unique_key varchar,
                                file_row_number integer,
                                file_record_unique_key varchar, ' || '\n';
    create_table_tail := ') enable_schema_evolution = ' || :schema_evolution ||';';

    get_table_ddl := 'select column_name, type from ' || :admin_schema || '.' || :control_table || ' order by order_id;';
    table_ddl := (execute immediate get_table_ddl);

    -- HERE WE WILL USE STANDARD CONVENTIONS FOR COLUMN NAMES, AND SPECIFICALLY PROVIDE DATATYPES
    for record in table_ddl do
        column_name := upper(replace(replace(record.column_name, ' ', '_'), '+', 'and'));

        column_datatype := case when record.type = 'TEXT' 
                                    then 'VARCHAR'
                                when regexp_like(record.type, '^NUMBER\\(\\d+, \\d+\\)$') 
                                    then 'NUMBER(38, ' || regexp_substr(record.type, '^NUMBER\\(\\d+, (\\d+)\\)$', 1, 1, 'e', 1) || ')'
                                else record.type
                            end;
        column_ddl := :column_name || ' ' || :column_datatype || ',' || '\n';
        create_table_body := :create_table_body || :column_ddl;
    end for;

    create_table_body := rtrim(create_table_body, ',\n');

    create_table_statement := :create_table_head || :create_table_body || :create_table_tail;
    -- return create_table_statement;
    execute immediate create_table_statement;

    return (select * from table(result_scan(last_query_id())));
    
end;


-- CHECK PROCESS OR CALL PROCEDURES
-------------------------------------------------------
-- That's it, we're done!  Now to see the process work do the following

-- Load files:
    -- Via one of the snowcli tools or manually through the stage UI, load our the following to raw_bronze.daily_files
        -- LOAN_MONTHLY_202601.csv.gz
        -- LOAN_MONTHLY_202602.csv.gz
        -- LOAN_MONTHLY_202603.csv.gz
    -- Then refresh the stage, i.e., "alter stage raw_bronze.daily_files refresh;"

-- Trigger the procedures:
call raw_bronze.loan_monthly_copy_into_raw_bronze();
call transform_silver.loan_monthly_merge_into_target_gold();

-- Check process output:
select * from target_gold.target_loan_monthly 
order by servicer_name, updated_at, loan_id;


-- To initiate the automated file ingest process, run all in the task_dag.sql worksheet. 


    -- Include steps for adding procedures
    -- Create the logging table
    -- Create and refine your procedures
        -- Include a check for new file step
        -- Include exceptions
        -- Include simple logging    
    -- Run everything
    -- Clean up the git repo
    -- Make a video
    

