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

-- CREATE THE LOGGING TABLE. THIS WILL BE STATICALLY TYPED.  A COPY OF THE PRINTOUT WILL BE PROVIDED IN THE GIT REPO
-- drop table admin_schema.loan_monthly_audit_history_table;
create table if not exists admin_schema.loan_monthly_audit_history_table (
    run_id varchar default uuid_string(),
    start_time timestamp_tz default current_timestamp(), 
    end_time timestamp_tz,
    processing_time_HHMMSS time,
    procedure_run_report varchar,
    exception_messages varchar
    );

insert into admin_schema.loan_monthly_audit_history_table (procedure_run_report)
    values('Starting LOAN_MONTHLY ingestion process'); 

select object_construct(*) 
from admin_schema.loan_monthly_audit_history_table
order by start_time desc
limit 1;


-- CREATE A TEST STAGE, FILES WILL BE MANUALLY LOADED
-- -- drop stage admin_schema.testing_files;
create stage if not exists admin_schema.testing_files 
directory = (
    enable = true
    refresh_on_create = true
    -- auto_refresh = true  -- Only works for external stages
    );

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

--STOP_HERE:  You will need to upload the test file 'LOAN_MONTHLY_202601.csv.gz' to the stage at admin_schema.testing_files  

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

-- select * from raw_bronze.inbound_loan_monthly_files_stream;
-- select * from admin_schema.loan_monthly_trigger_reset_temp_table;
    

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


-- USE THE CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR LANDING TABLE.  THIS MAY SEEM LIKE OVERKILL WITH LESS THAN TEN
-- COLUMNS IN OUR DATA MODEL, BUT THESE PORTABLE AND CAN BE USED ANYWHERE.  
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

-- NOTE: FOR EFFICIENCY SAKE, THERE ARE NO "SEPARATE" OR "REDUNDANT" DATA QUALITY CHECKS USED AS PART OF THIS EXCERCISE.  INSTEAD, WE DO OUR DATA QUALITY 
-- CHECKS AND FILTER OUT BAD RECORDS AT OUR TRANSFORMATION STEP USING THE CTE CASCADE.  NOTES ABOUT THIS ARE PROVIDED AT EACH STEP IN THE CASCASD; THIS IS A 
-- ROBUST APPROACH.  ALSO, THERE IS A DISCUSSION AT THE LAST CTE ABOUT HOW TO MANAGE UPDATES:  WITH THIS DATA SET IS SEEMS LIKE USING THE "UPSERT" (A NEW RECORD WITH UPDATED DATA)
-- WOULD BE MORE EFFICIENT AND LESS ERROR PRONE THAN MANUALLY PERFORMING INSERT/UPDATE STATEMENTS ON EXISTING RECORDS.

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
        -- LOADED_AT METADATA WAS CREATED BY OUR PROCESS.  LOGICALLLY THE MOST RECENT LOAD WOULD HAVE THE MOST RECENT RECORD.  BUT VENDOR DATA INTEGRITY IS NOT PERFECT SO WE WILL NOT USE IT
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


-- CREATE OUR COPY INTO RAW_BRONZE PROCEDURE
-------------------------------------------------------
use schema identifier($raw_schema);

-- drop procedure raw_bronze.loan_monthly_copy_into_raw_bronze();
-- create procedure if not exists raw_bronze.loan_monthly_copy_into_raw_bronze()
-- returns string
-- language sql
-- as
-- $$

declare
    -- Environment variables
    db varchar default 'HOMEWORK_ASSIGNMENT';
    admin_schema varchar default 'ADMIN_SCHEMA';
    admin_file_format varchar default 'INFER_SCHEMA_PIPE_DELIMITED';
    -- We will use "identifier" nomeclature for the logging table
    logging_table varchar default :admin_schema || '.' || 'LOAN_MONTHLY_AUDIT_HISTORY_TABLE';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    raw_schema varchar default 'RAW_BRONZE';
    table_name varchar default 'RAW_LOAN_MONTHLY';
    -- These objects are better non-concatenated as well
    file_stage varchar default '@' || :raw_schema || '.' || 'DAILY_FILES';
    file_format varchar default :raw_schema || '.' || 'INGEST_DATA_PIPE_DELIMITED';
    file_target_table varchar default 'TARGET_GOLD.TARGET_LOAN_MONTHLY';
    -- INSTEAD OF A FILE LIST LIKE BELOW WE CAN USE A FILE PATTERN:
    -- file_pattern_date varchar default to_varchar(dateadd('MONTH', -1, current_date()), 'YYYYMM');
    -- file_pattern varchar default 'LOAN_MONTHLY' || '_' || :file_pattern_date || '.csv.gz';
    files_array array;
    file_target varchar;
    file_schema_compare_query varchar;
    file_schema_compare_array array;
    file_schema_failures_array array default [];
    files_array_cleaned array;
    files_list varchar default '';
    
    -- Dynamic DDL/DML variables
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
    copy_into_errors boolean;

    -- Process logging variables
    procedure_metadata variant;
    query_id varchar default '';
    run_id varchar default '';
    start_time timestamp_tz;
    end_time timestamp_tz;
    processing_time time;
    procedure_step_result varchar default '';
    procedure_run_report varchar default '';
    return_statement varchar default '';

    -- Custome no daily files exception
    no_daily_files exception;

    -- Custome no daily files exception
    file_schema_mismatch exception;

    -- Custom copy into exception
    copy_into_failure exception;
    copy_into_error_message varchar default '';
    copy_into_history varchar default '';
    -- Other type exception handling variables
    sql_error_message varchar default '';

begin

    -- Generate logging metadata;
    insert into identifier(:logging_table) (procedure_run_report)
        values('Starting LOAN_MONTHLY ingestion process');
    
    -- Retrieve logging metadata;
    select object_construct(*) into procedure_metadata
    from identifier(:logging_table)
    order by start_time desc
    limit 1;

    run_id := :procedure_metadata['RUN_ID'];
    start_time := to_timestamp_tz(:procedure_metadata['START_TIME']);
    procedure_run_report := :procedure_metadata['PROCEDURE_RUN_REPORT'];


    -- Begin procedure steps
    procedure_run_report := :procedure_run_report || '\n'|| '  --Beginning copy into step';



    files_array := (select array_agg(d.relative_path) within group (order by d.relative_path) 
                    from directory(@raw_bronze.daily_files) d 
                    left join identifier(:file_target_table) t 
                        on d.relative_path = t.file_name 
                    where t.file_name is null);

    if (array_size(files_array) = 0) then
        raise no_daily_files;
    else null;
    end if;

    files_array_cleaned := files_array;
    for i in 0 to array_size(files_array) - 1 do
        file_target := files_array[i];

        file_schema_compare_query := 'select array_agg(object_construct(*))
                                      from (select   
                                                ''' || :file_target || ''' as inbound_file_name,
                                                nf.column_name as inbound_file_column_name,
                                                nf.order_id as inbound_file_order_id,
                                                iff(regexp_like(nf.type, ''^NUMBER\\\\(\\\\d+, \\\\d+\\\\)$''),
                                                    ''NUMBER(38, '' || regexp_substr(nf.type, ''^NUMBER\\\\(\\\\d+, (\\\\d+)\\\\)$'', 1, 1, ''e'', 1) || '')'',    
                                                    nf.type) as inbound_file_datatype,
                                                ''' || :control_table || ''' as admin_file_name,
                                                ct.column_name as admin_file_schema_column_name,
                                                ct.order_id as admin_file_schema_order_id,
                                                iff(regexp_like(ct.type, ''^NUMBER\\\\(\\\\d+, \\\\d+\\\\)$''),
                                                    ''NUMBER(38, '' || regexp_substr(ct.type, ''^NUMBER\\\\(\\\\d+, (\\\\d+)\\\\)$'', 1, 1, ''e'', 1) || '')'',    
                                                    ct.type) as admin_file_schema_datatype,
                                            from table(infer_schema(location => ''' || :file_stage || ''',
                                                                  file_format => ''' || :admin_schema || '.' || :admin_file_format || ''',
                                                                  files => ''' || :file_target || ''',
                                                                  ignore_case => false)) nf
                                            full outer join ' || :admin_schema || '.' || :control_table || ' ct
                                                on nf.column_name = ct.column_name and nf.type = ct.type
                                            where 
                                                inbound_file_column_name is null
                                                    or
                                                admin_file_schema_column_name is null);';
        execute immediate file_schema_compare_query;
        query_id := last_query_id();
        file_schema_compare_array := (select * from table(result_scan(:query_id)));
        
        if (array_size(file_schema_compare_array) = 0) then
            null;
        else
            files_array_cleaned := array_remove(:files_array_cleaned, to_variant(:file_target));
            file_schema_failures_array := array_cat(:file_schema_failures_array, :file_schema_compare_array);
        end if;
        
    end for;

    if (array_size(file_schema_failures_array) = 0) then
        procedure_run_report := :procedure_run_report || '\n' || '    ----All files passed file schema check';
    else
        if (array_size(files_array_cleaned) = 0) then
            procedure_run_report := :procedure_run_report || '\n' || '    ----All files failed file schema check. '
                                                          || 'Check exception_messages column in the admin_schema.loan_monthly_audit_history table';
            raise file_schema_mismatch;
            -- Process should stop here
        else
            procedure_run_report := :procedure_run_report || '\n' || '    ----Some files failed file schema check.  Processing will continue. '
                                                          || 'Check exception_messages column in the admin_schema.loan_monthly_audit_history table';

            -- Just do an insert here                                              
        end if;

    end if;

    -- TO DO:
        -- create your new exceptions
        -- add your insert statement for file exception


    files_list := '''' || array_to_string(:files_array_cleaned, ''', ''') || '''';
        
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
    
    copy_into_tail := 'from ' || :file_stage || ')
                       files = (' || :files_list || ')
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

    query_id := last_query_id();
    procedure_step_result := (select array_agg(object_construct(*)) from table(result_scan(:query_id)));

    -- HERE IS OUR CHECK AND EXCEPTION HANDLING FOR COPY INTO ERRORS
    copy_into_errors := (select count(*) > 0
                         from table(flatten(parse_json(:procedure_step_result)))
                         where to_varchar(value:status) != 'LOADED');

    if (copy_into_errors = true) then 
        raise copy_into_failure;
    else null;
    end if;
    
    procedure_run_report := procedure_run_report || '\n' || '    ----Query id: ' || :query_id || '; Result: ' ||:procedure_step_result;
    
    return_statement := '  --Success:  raw_bronze.loan_monthly_copy_into_raw_bronze() complete. ' || '\n' ||
                        '    ----LOAN_MONTHLY ingestion process will continue...';

    procedure_run_report := procedure_run_report || '\n' || :return_statement;

    update identifier(:logging_table)
        set procedure_run_report = :procedure_run_report
    where run_id = :run_id;

    return return_statement;
    
exception 

    when copy_into_failure then
        query_id := last_query_id();
        copy_into_history := procedure_step_result;
        copy_into_error_message := 'Copy into failure: not all rows successfully loaded';
        end_time := current_timestamp();
        processing_time := timeadd(second, timestampdiff(second, start_time, end_time), to_time('00:00:00'));
        
        procedure_step_result := '    ----Procedure exception: ' || :copy_into_error_message || '\n' ||
                                 '    ----Procedure will terminate.';
        procedure_run_report := :procedure_run_report || '\n' || :procedure_step_result;

        return_statement := '  --Failure: raw_bronze.loan_monthly_copy_into_raw_bronze() did not complete. ' || '\n' ||
                            '    ----Exception message: ' || :copy_into_error_message || '\n' ||
                            '    ----Copy into history: ' || :copy_into_history || '\n' ||
                            '  --LOAN_MONTHLY ingestion process terminated' || '\n' ||
                            'Check the logs at admin_schema.loan_monthly_audit_history_table for run_id: '|| :run_id;
        procedure_run_report := :procedure_run_report || '\n' || :return_statement;

        update identifier(:logging_table)
        set end_time = :end_time,
            processing_time_HHMMSS = :processing_time,
            procedure_run_report = :procedure_run_report,
            exception_messages = :copy_into_error_message
        where run_id = :run_id;

        return return_statement;

    when other then
        query_id := last_query_id();
        sql_error_message := sqlerrm;
        end_time := current_timestamp();
        processing_time := timeadd(second, timestampdiff(second, start_time, end_time), to_time('00:00:00'));
        
        procedure_step_result := '    ----Procedure exception: ' || :sql_error_message || '\n' ||
                                 '    ----Procedure will terminate.';
        procedure_run_report := :procedure_run_report || '\n' || :procedure_step_result;

        return_statement := '  --Failure: raw_bronze.loan_monthly_copy_into_raw_bronze() did not complete. ' || '\n' ||
                            '    ----Exception message: ' || :sql_error_message || '\n' ||
                            '  --LOAN_MONTHLY ingestion process terminated' || '\n' ||
                            'Check the logs at admin_schema.loan_monthly_audit_history_table for run_id: '|| :run_id;
        procedure_run_report := :procedure_run_report || '\n' || :return_statement;

        update identifier(:logging_table)
        set end_time = :end_time,
            processing_time_HHMMSS = :processing_time,
            procedure_run_report = :procedure_run_report,
            exception_messages = :sql_error_message
        where run_id = :run_id;

        return return_statement;

end;

$$;


-- CREATE OUR MERGE INTO TARGET_GOLD PROCEDURE
-------------------------------------------------------
use schema identifier($target_schema);

-- drop procedure transform_silver.loan_monthly_merge_into_target_gold();
create procedure if not exists transform_silver.loan_monthly_merge_into_target_gold()
returns string
language sql
as
$$

declare
    -- Environment variables
    db varchar default 'HOMEWORK_ASSIGNMENT';
    admin_schema varchar default 'ADMIN_SCHEMA';
    -- Logging table queries will use "identifier" nomenclature
    logging_table varchar default :admin_schema || '.' || 'LOAN_MONTHLY_AUDIT_HISTORY_TABLE';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    information_schema varchar default 'INFORMATION_SCHEMA';
    is_table varchar default 'COLUMNS';
    source_schema varchar default 'TRANSFORM_SILVER';
    source_table varchar default 'VW_LOAN_MONTHLY_CLEAN';
    target_schema varchar default 'TARGET_GOLD';
    target_table varchar default 'TARGET_LOAN_MONTHLY';
    match_key varchar default 'FILE_RECORD_UNIQUE_KEY';

    -- Dynamic DDL/DML variables
    merge_statement_head varchar default '';
    column_metadata_query varchar default '';
    column_metadata resultset;
    source_column_name varchar default '';
    source_column_datatype_transform varchar default '';
    target_column_name varchar default '';
    source_column_dml varchar default '';
    insert_dml varchar default '';
    target_column_dml varchar default '';
    values_dml varchar default '';
    merge_into_statement default '';

    -- Process logging variables
    metadata_query varchar;
    procedure_metadata variant;
    query_id varchar default '';
    run_id varchar default '';
    start_time timestamp_tz;
    end_time timestamp_tz;
    processing_time time;
    procedure_step_result varchar default '';
    procedure_run_report varchar default '';
    return_statement varchar default '';

    -- Exception handling variables
    sql_error_message varchar default '';
    
begin

    -- Retrieve logging metadata;
    select object_construct(*) into procedure_metadata
    from identifier(:logging_table)
    order by start_time desc
    limit 1;
    

    run_id := :procedure_metadata['RUN_ID'];
    start_time := to_timestamp_tz(:procedure_metadata['START_TIME']);
    procedure_run_report := :procedure_metadata['PROCEDURE_RUN_REPORT'];

    
    -- Begin procedure steps
    procedure_run_report := :procedure_run_report || '\n'|| '  --Beginning merge step';
    
    merge_statement_head := 'merge into ' || :target_schema || '.' || :target_table || ' t 
                             using (select * from ' || :source_schema || '.' || :source_table || ' ) s
                             on t.' || :match_key || ' = ' || 's.'|| :match_key || '
                             when not matched then 
                                insert(';

    column_metadata_query := 'select 
                                    isc.column_name as isc_column_name,
                                    isc.data_type_alias as isc_data_type_alias,
                                    -- USE THE EXACT SAME TRANSFORM AS WE DID WHEN CREATING THE TARGET_LOAN_MONTHLY TABLE
                                    iff(act.column_name is not null,
                                        act.column_name,
                                        isc.column_name) as act_column_name,
                                    upper(replace(replace(act_column_name, '' '', ''_''), ''+'', ''and'')) as act_join_column_name,
                                    act.type as act_type,
                              from ' || :information_schema || '.' || :is_table || ' isc
                              left join ' || :admin_schema || '.' || :control_table || ' act
                                on 
                                    isc.column_name = act_join_column_name 
                              where 
                                    isc.table_schema = ''' || :target_schema || '''
                                        and
                                    isc.table_name = ''' || :target_table || '''
                              order by ordinal_position;';
    column_metadata := (execute immediate column_metadata_query);
    -- return table(column_metadata);
    
    for record in column_metadata do
        target_column_name := 't.' || record.isc_column_name;
        source_column_name := 's.' || '"' || record.act_column_name || '"'; 
        source_column_datatype_transform := record.isc_data_type_alias;
        
        target_column_dml := :target_column_name || ',' || '\n';
        source_column_dml := :source_column_name || '::' || :source_column_datatype_transform || ',' || '\n';
        
        insert_dml := :insert_dml || :target_column_dml;
        values_dml := :values_dml || :source_column_dml;
        
    end for;
    
    insert_dml := rtrim(insert_dml, ',\n');
    values_dml := rtrim(values_dml, ',\n');

    merge_into_statement := :merge_statement_head || :insert_dml || ')' || '\n' || 'values(' || :values_dml || ');';    
    execute immediate merge_into_statement;

    query_id := last_query_id();
    procedure_step_result := (select array_agg(object_construct(*)) from table(result_scan(:query_id)));
    procedure_run_report := procedure_run_report || '\n' || '    ----Query id: ' || :query_id || '; Result: ' ||:procedure_step_result;

    end_time := current_timestamp();
    processing_time := timeadd(second, timestampdiff(second, start_time, end_time), to_time('00:00:00'));
    
    return_statement := '  --Success:  transform_silver.loan_monthly_merge_into_target_gold() complete. ' || '\n' ||
                        'LOAN_MONTHLY ingestion process successfully completed' || '\n' ||
                        'Check the logs at admin_schema.loan_monthly_audit_history_table for run_id: '|| :run_id;

    procedure_run_report := procedure_run_report || '\n' || :return_statement;

    update identifier(:logging_table)
        set end_time = :end_time,
            processing_time_HHMMSS = :processing_time,
            procedure_run_report = :procedure_run_report
        where run_id = :run_id;

    return return_statement;

exception 
    when other then
        query_id := last_query_id();
        sql_error_message := sqlerrm;
        end_time := current_timestamp();
        processing_time := timeadd(second, timestampdiff(second, start_time, end_time), to_time('00:00:00'));
        
        procedure_step_result := '    ----Procedure exception: ' || :sql_error_message || '\n' ||
                                 '    ----Procedure will terminate.';
        procedure_run_report := :procedure_run_report || '\n' || :procedure_step_result;

        return_statement := '  --Failure: transform_silver.loan_monthly_merge_into_target_gold() did not complete. ' || '\n' ||
                            '    ----Exception message: ' || :sql_error_message || '\n' ||
                            'LOAN_MONTHLY ingestion process did not complete' || '\n' ||
                            'Check the logs at admin_schema.loan_monthly_audit_history_table for run_id: '|| :run_id;
        procedure_run_report := :procedure_run_report || '\n' || :return_statement;

        update identifier(:logging_table)
        set end_time = :end_time,
            processing_time_HHMMSS = :processing_time,
            procedure_run_report = :procedure_run_report,
            exception_messages = :sql_error_message
        where run_id = :run_id;

        return return_statement;

end;

$$;

-- CHECK PROCESS OR CALL PROCEDURES
-------------------------------------------------------
-- That's it, we're done!  Now to see the process work do the following

-- Load files:
    -- Via one of the snowcli tools or manually through the stage UI, load our the following to raw_bronze.daily_files
        -- LOAN_MONTHLY_202601.csv.gz
        -- LOAN_MONTHLY_202602.csv.gz
        -- LOAN_MONTHLY_202603.csv.gz
    -- Example: 
            -- tommy@fedora:~$ export PRIVATE_KEY_PASSPHRASE="your passphrase"
            -- tommy@fedora:~$ snow stage copy "/home/tommy/DevelopmentWork/Homework/LOAN_MONTHLY_2026*.csv.gz" @homework_assignment.raw_bronze.daily_files -c homework_assignment --auto-compress
    -- Then refresh the stage, i.e., "alter stage raw_bronze.daily_files refresh;"

-- Trigger the procedures:
    -- call raw_bronze.loan_monthly_copy_into_raw_bronze();
    -- call transform_silver.loan_monthly_merge_into_target_gold();

-- Check process output:
    select * from target_gold.target_loan_monthly 
    order by servicer_name, updated_at, loan_id;

    select * from admin_schema.loan_monthly_audit_history_table;


-- To initiate the automated file ingest process, run all in the task_dag.sql worksheet. 

    -- Create and refine your procedures
        -- Include a check for file schema changes step   
        -- Test merge failure on datatype
    -- Run everything
    -- Clean up the git repo
    -- Make a video
    

