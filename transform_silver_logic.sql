use database homework_assignment;
use schema transform_silver;


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


