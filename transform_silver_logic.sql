use database homework_assignment;
use schema transform_silver;



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
        select *
        from compile_good_records
        qualify row_number() over (partition by "loan_id" order by "updated_at" desc) = 1
    )
    select * from select_best_record_for_loan_id order by "servicer_name", "loan_id", "updated_at" desc
    );

select * from transform_silver.vw_loan_monthly_clean; 


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
                                insert(';

    column_metadata_query := 'select 
                                    act.column_name as act_column_name,
                                    act.type as act_type,
                                    -- USE THE EXACT SAME TRANSFORM AS WE DID WHEN CREATING THE TARGET_LOAN_MONTHLY TABLE
                                    upper(replace(replace(act_column_name, '' '', ''_''), ''+'', ''and'')) as act_join_column_name,
                                    isc.column_name as isc_column_name,
                                    isc.data_type_alias as isc_data_type_alias
                              from ' || :admin_schema || '.' || :control_table || ' act
                              left join ' || :information_schema || '.' || :is_table || ' isc
                                on 
                                    act_join_column_name = isc.column_name
                                and 
                                    isc.table_schema = ''' || :target_schema || '''
                                and
                                    isc.table_name = ''' || :target_table || '''
                              order by order_id;';
    column_metadata := (execute immediate column_metadata_query);
    --return table(column_metadata);
    
    for record in column_metadata do
        source_column_name := 's.' || '"' || record.act_column_name || '"'; 
        source_column_datatype_transform := record.isc_data_type_alias;
        target_column_name := 't.' || record.isc_column_name;

        source_column_dml := :source_column_name || '::' || :source_column_datatype_transform || ',' || '\n';
        target_column_dml := :target_column_name || ',' || '\n';

        values_dml := :values_dml || :source_column_dml;
        insert_dml := :insert_dml || :target_column_dml;
        
    end for;
    
    values_dml := rtrim(values_dml, ',\n');
    insert_dml := rtrim(insert_dml, ',\n');

    merge_into_statement := :merge_statement_head || :insert_dml || ')' || '\n' || 'values(' || :values_dml || ');';
    -- return merge_into_statement;
    
    execute immediate merge_into_statement;
    
    return (select array_agg(object_construct(*)) from table(result_scan(last_query_id())));
    
end;


