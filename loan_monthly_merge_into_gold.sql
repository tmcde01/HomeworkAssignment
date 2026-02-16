use database homework_assignment;
use schema transform_silver;

-- HERE IS OUR MERGE INTO PROCEDURE WE WILL CALL VIA TASK DAG.  WE INCLUDE ONLY A SINGLE EXCEPTION BECAUSE
-- WE COVERED EXCEPTION EXAMPLES IN THE COPY INTO PROCEDURE.  WE WILL TEST IT THOUGH, INCLUDING MERGE FAILURES.

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
                             -- WE STILL NEED OUR METADATA
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
                            '    ----Exception_message: ' || :sql_error_message || '\n' ||
                            'LOAN_MONTHLY ingestion process did not complete' || '\n' ||
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


