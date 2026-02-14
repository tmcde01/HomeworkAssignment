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

-- COPY SOME FILES INTO IT
copy files 
    into @raw_bronze.daily_files 
    from @admin_schema.testing_files
    pattern = '^.*\LOAN_MONTHLY_\\d{6}\\.csv\\.gz$';

select * from directory(@raw_bronze.daily_files);


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


-- NOW USE THE SAME CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR COPY INTO STATEMENT
-- drop table raw_bronze.raw_loan_monthly;

-- USE THE CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR LANDING TABLE
declare
    db varchar default 'HOMEWORK_ASSIGNMENT';
    admin_schema varchar default 'ADMIN_SCHEMA';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    raw_schema varchar default 'RAW_BRONZE';
    table_name varchar default 'RAW_LOAN_MONTHLY';

    file_stage varchar default :raw_schema || '.' || 'DAILY_FILES';
    file_format varchar default :raw_schema || '.' || 'INGEST_DATA_PIPE_DELIMITED';
    -- FOR TESTING WE WILL MANIPULATE THE FILE PATTERN DATE:
    -- file_pattern_date varchar default to_varchar(dateadd('MONTH', -1, current_date()), 'YYYYMM');
    file_pattern_date varchar default '(202601|202602|202603)';
    file_pattern varchar default 'LOAN_MONTHLY' || '_' || :file_pattern_date || '.csv.gz';
    copy_into_head varchar default '';
    copy_into_tail varchar default '';

    get_table_ddl varchar;
    table_ddl resultset;
    column_reference varchar default '';
    column_datatype varchar default '';
    column_name varchar default '';
    column_dml varchar default '';
    copy_into_body varchar default '';
    
    copy_into_statement varchar; 

begin

    copy_into_head := 'copy into ' || :raw_schema || '.' || :table_name || '
                            from (select 
                                        metadata$start_scan_time as loaded_at,
                                        metadata$filename as file_name,
                                        to_varchar(to_date(regexp_substr(metadata$filename, ''^.*LOAN_MONTHLY_(\\\\d{6})\\\\.csv\\\\.gz$'', 1, 1, ''e'', 1), 
                                            ''YYYYMM''), ''YYYY-MM'') as file_month,
                                        md5(loaded_at || file_name) as file_unique_key,
                                        metadata$file_row_number as file_row_number,
                                        md5(loaded_at || file_name || file_row_number) as file_record_unique_key,' || '\n';
    -- return copy_into_head;



-- LOAN_MONTHLY_202602.csv.gz
-- select to_varchar(to_date(regexp_substr('LOAN_MONTHLY_202602.csv.gz', '^.*LOAN_MONTHLY_(\\d{6})\\.csv\\.gz$', 1, 1, 'e', 1), 
--                                             'YYYYMM'), 'YYYY-MM') as file_month,

    
    copy_into_tail := 'from @' || :file_stage || ')
                       pattern = ''' || :file_pattern || '''
                       file_format = ' || :file_format || ';';
    -- return copy_into_tail;
                

    get_table_ddl := 'select order_id, type, column_name from ' || :admin_schema || '.' || :control_table || ' order by order_id;';
    table_ddl := (execute immediate get_table_ddl);

    for record in table_ddl do
        column_reference := '$' || to_varchar(record.order_id + 1);
        -- FORCE EVERYTHING TO VARCHAR
        column_datatype := case when record.type = 'TEXT' 
                                    then 'VARCHAR'
                                when regexp_like(record.type, '^NUMBER\\(\\d+, \\d+\\)$') 
                                    then 'VARCHAR'
                                    -- then 'NUMBER(38, ' || regexp_substr(record.type, '^NUMBER\\(\\d+, (\\d+)\\)$', 1, 1, 'e', 1) || ')'
                                when record.type = 'TIMESTAMP_NTZ' 
                                    then 'VARCHAR'
                           else 'VARCHAR'
                           end;
        column_name := '"' || record.column_name || '"';
        column_dml := :column_reference || '::' || :column_datatype || ' as ' || :column_name || ',' || '\n';
        copy_into_body := :copy_into_body || :column_dml;
    end for;

    copy_into_body := rtrim(copy_into_body, ',\n');
    -- return copy_into_body;

    copy_into_statement := :copy_into_head || :copy_into_body || :copy_into_tail;
    -- return copy_into_statement;
    execute immediate copy_into_statement;
    
    return (select array_agg(object_construct(*)) from table(result_scan(last_query_id())));

end;

select * from HOMEWORK_ASSIGNMENT.RAW_BRONZE.RAW_LOAN_MONTHLY;



-- WORK WITHIN TRANSFORM_SILVER SCHEMA
-------------------------------------------------------
use schema identifier($transform_schema);

-- THIS WILL BE ADDRESSED IN A SEPARATE WORKSHEET BECAUSE OF SOME OF THE COMPLEXITIES INVOLVED:
    -- Instead of Data Quality checks, we will use filtering and failover tables to eliminate bad records
    -- The assignment seems to be asking to create a view at the VW_LOAN_MONTHLY_CLEAN layer, to be used to merge data to the TARGET_LOAN_MONTHLY (gold) layer
        -- The issue with this is a view is a stored query that does not persist data:  this means to load to the TARGET_LOAN_MONTHLY we would use a view to support the merge statement
        -- Since the view will contain the transformation logic, we would apply it to every record in the RAW_LOAN_MONTHLY (bronze) layer each time the process runs.  
        -- Thus we would be applying the same filtering and processing to the same records over and over, and as the raw layer grows the compute will grow too.
        -- A better idea might be to persist cleaned data in a table at the LOAN_MONTHLY_CLEAN layer (no VW),  Duplicate or bad records could be purged from the raw layer, and the merge to the target layer             -- would be more performant.   



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
                                file_month date,
                                file_unique_key varchar,
                                file_row_number integer,
                                file_record_unique_key varchar, ' || '\n';
    create_table_tail := ') enable_schema_evolution = ' || :schema_evolution ||';';

    get_table_ddl := 'select column_name, type from ' || :admin_schema || '.' || :control_table || ' order by order_id;';
    table_ddl := (execute immediate get_table_ddl);

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

select * from target_gold.target_loan_monthly;
