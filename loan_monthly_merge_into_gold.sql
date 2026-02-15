use database homework_assignment;
use schema transform_silver;

-- HERE IS OUR MERGE INTO PROCEDURE WE WILL CALL VIA TASK DAG

-- drop procedure transform_silver.loan_monthly_merge_into_target_gold();
create procedure if not exists transform_silver.loan_monthly_merge_into_target_gold()
returns string
language sql
as
-- $$

declare
    db varchar default 'HOMEWORK_ASSIGNMENT';
    admin_schema varchar default 'ADMIN_SCHEMA';
    control_table varchar default 'LOAN_MONTHLY_EXPECTED_FILE_SCHEMA';
    information_schema varchar default 'INFORMATION_SCHEMA';
    is_table varchar default 'COLUMNS';
    source_schema varchar default 'TRANSFORM_SILVER';
    source_table varchar default 'VW_LOAN_MONTHLY_CLEAN';
    target_schema varchar default 'TARGET_GOLD';
    target_table varchar default 'TARGET_LOAN_MONTHLY';
    match_key varchar default 'FILE_RECORD_UNIQUE_KEY';
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
    
begin

    execute immediate ('use database ' || :db);
    execute immediate ('use schema ' || :source_schema);


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
    
    return (select array_agg(object_construct(*)) from table(result_scan(last_query_id())));
    
end;




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

$$;