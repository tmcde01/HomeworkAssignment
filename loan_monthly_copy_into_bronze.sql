use database homework_assignment;
use schema raw_bronze;

-- HERE IS OUR COPY INTO PROCEDURE WE WILL CALL VIA TASK DAG



-- NOW USE THE SAME CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR COPY INTO STATEMENT
-- drop table raw_bronze.raw_loan_monthly;


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
    -- We will use "identifier" nomeclature for the logging table
    logging_table varchar default :admin_schema || '.' || 'LOAN_MONTHLY_AUDIT_HISTORY_TABLE';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    raw_schema varchar default 'RAW_BRONZE';
    table_name varchar default 'RAW_LOAN_MONTHLY';
    -- These objects are better non-concatenated as well
    file_stage varchar default :raw_schema || '.' || 'DAILY_FILES';
    file_format varchar default :raw_schema || '.' || 'INGEST_DATA_PIPE_DELIMITED';
    -- FOR TESTING WE WILL MANIPULATE THE FILE PATTERN DATE:
    -- file_pattern_date varchar default to_varchar(dateadd('MONTH', -1, current_date()), 'YYYYMM');
    file_pattern_date varchar default '(202601|202602|202603)';
    file_pattern varchar default 'LOAN_MONTHLY' || '_' || :file_pattern_date || '.csv.gz';
    
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

    -- Custom copy into exception
    copy_into_failure exception;
    -- Other type exception handling variables
    sql_error_message varchar default '';

begin

-- Set environment
    execute immediate ('use database ' || :db);
    execute immediate ('use schema ' || :raw_schema);

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


    -- Being procedure steps
    procedure_run_report := :procedure_run_report || '\n'|| '  --Beginning copy into step';


-- TO DO:  
    -- check for new files
    -- parse file row ingestion
    -- add exception.

    if (rows_loaded = 0) then
        raise copy_into_failure;
    end if;


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

    query_id := last_query_id();
    procedure_step_result := (select array_agg(object_construct(*)) from table(result_scan(:query_id)));
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
        sql_error_message := sqlerrm;
        end_time := current_timestamp();
        processing_time := timeadd(second, timestampdiff(second, start_time, end_time), to_time('00:00:00'));
        
        procedure_step_result := '    ----Procedure exception: ' || :sql_error_message || '\n' ||
                                 '    ----Procedure will terminate.';
        procedure_run_report := :procedure_run_report || '\n' || :procedure_step_result;

        return_statement := '  --Failure: raw_bronze.loan_monthly_copy_into_raw_bronze() did not complete. ' || '\n' ||
                            '    ----Exception_message: ' || :sql_error_message || '\n' ||
                            '  --LOAN_MONTHLY ingestion process terminated' || '\n' ||
                            'Check the logs at admin_schema.loan_monthly_audit_history_table for run_id: '|| :run_id;
        procedure_run_report := :procedure_run_report || '\n' || :return_statement;

        update identifier(:logging_table)
        set end_time = :end_time,
            processing_time_HHMMSS = :processing_time,
            procedure_run_report = :procedure_run_report,
            exception_message = :sql_error_message
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
                            '    ----Exception_message: ' || :sql_error_message || '\n' ||
                            '  --LOAN_MONTHLY ingestion process terminated' || '\n' ||
                            'Check the logs at admin_schema.loan_monthly_audit_history_table for run_id: '|| :run_id;
        procedure_run_report := :procedure_run_report || '\n' || :return_statement;

        update identifier(:logging_table)
        set end_time = :end_time,
            processing_time_HHMMSS = :processing_time,
            procedure_run_report = :procedure_run_report,
            exception_message = :sql_error_message
        where run_id = :run_id;

        return return_statement;

end;

$$;

$$



select * from admin_schema.loan_monthly_audit_history_table order by start_time desc;



-- create table if not exists admin_schema.loan_monthly_audit_history_table (
--     run_id varchar default uuid_string(),
--     start_time timestamp_tz default current_timestamp(), 
--     end_time timestamp_tz,
--     processing_time time,
--     procedure_run_report varchar,
--     exception_messages variant
--     );

