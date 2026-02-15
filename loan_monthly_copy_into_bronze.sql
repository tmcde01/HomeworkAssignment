use database homework_assignment;
use schema raw_bronze;

-- HERE IS OUR COPY INTO PROCEDURE WE WILL CALL VIA TASK DAG



-- NOW USE THE SAME CONTROL TABLE WITH AN ANONYMOUS BLOCK TO DYNAMICALLY CREATE OUR COPY INTO STATEMENT
-- drop table raw_bronze.raw_loan_monthly;


-- drop procedure raw_bronze.loan_monthly_copy_into_raw_bronze();
create procedure if not exists raw_bronze.loan_monthly_copy_into_raw_bronze()
returns string
language sql
as
-- $$

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




    RETURN 'SUCCESS -- LOAD_DATA_FROM_FILES() procedure complete.  Check the logs at GENERAL_HOSPITAL_PATIENTS.ADMIN_DAILY_INGEST_REPORTS';

    EXCEPTION
        WHEN OTHER THEN
            sql_error_message := SQLERRM;
            procedure_run_report := procedure_run_report || '\t'   || '--General failure of LOAD_DATA_FROM_FILES() procedure: ' || sql_error_message || '\n'
                                                         || '\t\t' || '--Procedure will terminate.' || '\n\n'
                                                         || '...End daily ingestion of patient data for files.' || '\n'
                                                         || 'Success: FALSE';
            INSERT INTO GENERAL_HOSPITAL_PATIENTS.ADMIN_DAILY_INGEST_REPORTS (FILE_NAME, DAILY_INGEST_RESULTS, EXCEPTION_SUMMARY)
                VALUES (:file_, :procedure_run_report, :sql_error_message);

            RETURN 'ERROR -- LOAD_DATA_FROM_FILES() procedure did not complete.  Check the logs at GENERAL_HOSPITAL_PATIENTS.ADMIN_DAILY_INGEST_REPORTS'
                    || ' for file-specific reports';



end;

$$;


