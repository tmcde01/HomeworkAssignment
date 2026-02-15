-- HERE IS THE SETUP WORKSHEET FOR OUR TASK DAG.  TASKS ARE USED AS WRAPPERS TO TRIGGER OUR INGEST PROCESS AND
-- MANAGE OUR INGEST PROCEDURES.  AT THE BOTTOM OF THE PAGE ARE SOME SAMPLE COMMANDS FOR USING USING TAGS ALONG 
-- WITH TASKS TO TRACK COMPUTE COSTS.  THESE ARE OMMITTED HERE FOR THIS DEMONSTRATION, THOUGH AN EXAMPLE IS PROVIDED.
-- WE WILL MAINTAIN THESE IN THE ADMIN SCHEMA, THOUGH WE COULD KEEP THEM ANYWHERE

use database homework_assignment;
use schema admin_schema;

-- This is our root task.  It uses the stream we created to check for a new file drop.  It runs serverless every
-- five minutes, which means we do not need to keep a warehouse running continuously.  

-- -- drop task admin_schema.loan_monthly_1_ingest_start;
create task if not exists admin_schema.loan_monthly_1_ingest_start
    schedule = 'using cron */5 * * * * America/Denver'
    when system$stream_has_data('raw_bronze.inbound_loan_monthly_files_stream')
        as 
            select true;

-- This is an admin task that resets our ingest process trigger by clearing the stream.  We do this by selecting
-- from the stream to a disposable temp table.  Here we are also using a warehouse now, even though there is minor
-- compute involved

-- -- drop task admin_schema.loan_monthly_2_trigger_reset;
create task if not exists admin_schema.loan_monthly_2_trigger_reset
    warehouse = 'compute_wh'
    after admin_schema.loan_monthly_1_ingest_start
    as
        create or replace temporary table admin_schema.loan_monthly_trigger_reset_temp_table
            as
                select * from raw_bronze.inbound_loan_monthly_files_stream;

-- Here we provide some sample tags to show how they might be used to track our copy into costs.  Note that the
-- task is used to call the underlying procedure.  This supports portability and composablility for our procedures.

-- -- drop task admin_schema.loan_monthly_3_copy_into_raw_bronze;
create task if not exists admin_schema.loan_monthly_3_copy_into_raw_bronze
    -- with tag (data_governance.costing_tags.environment = 'production',
    --           data_governance.costing_tags.job_id = 'loan_monthly_ingest',
    --           data_governance.costing_tags.task_id = 'call_loan_monthly_copy_into_raw_bronze') 
    warehouse = 'compute_wh'
    after admin_schema.loan_monthly_1_ingest_start
    as
        call raw_bronze.loan_monthly_copy_into_raw_bronze();

-- drop task admin_schema.loan_monthly_4_merge_into_target_gold;
create task if not exists admin_schema.loan_monthly_4_merge_into_target_gold
    warehouse = 'compute_wh'
    after admin_schema.loan_monthly_3_copy_into_raw_bronze
    as
        call transform_silver.loan_monthly_merge_into_target_gold();


 -- We will need to provide permissions for our sysadmin role to run the tasks:
use role accountadmin;
grant execute task on account to role sysadmin;
grant execute managed task on account to role sysadmin;
use role sysadmin;


 -- Now we need to manage our tasks by activating or deactivating them:  turning off the tasks turns off the process.
 -- Note there is a "gotcha" in that the root task must be started last
alter task admin_schema.loan_monthly_2_trigger_reset resume;
alter task admin_schema.loan_monthly_3_copy_into_raw_bronze resume;
alter task admin_schema.loan_monthly_4_merge_into_target_gold resume;
alter task admin_schema.loan_monthly_1_ingest_start resume;

-- Alternatively we could manage the whole task flow:
-- select system$task_dependents_enable('ADMIN_SCHEMA.LOAN_MONTHLY_1_INGEST_START');

alter task admin_schema.loan_monthly_1_ingest_start suspend;
alter task admin_schema.loan_monthly_2_trigger_reset suspend;
alter task admin_schema.loan_monthly_3_copy_into_raw_bronze suspend;
alter task admin_schema.loan_monthly_4_merge_into_target_gold suspend;


-- And that's it!  We could add other tasks to do things like clean up our landing layer, run data quality checks,
-- etc. The only downside to tasks is thay they do not provide the same query history or return statements as 
-- procedures, which can be fixed by using them to wrap procedures and building out logging, as we do here.



-- BELOW IS THE SETUP FOR USING COSTING TAGS
---------------------------------------------------------------
-- use role accountadmin;

-- -- drop database data_governance;
-- create database if not exists data_governance;
-- use database data_governance;

-- create schema if not exists costing_tags;
-- use schema costing_tags;

-- create tag if not exists data_governance.costing_tags.environment comment = 'tag to track environment costs';
-- create tag if not exists data_governance.costing_tags.job_id comment = 'tag to track job costs';
-- create tag if not exists data_governance.costing_tags.task_id comment = 'tag to track task costs';

-- grant usage on database data_governance to role sysadmin; 
-- grant usage on schema data_governance.costing_tags to role sysadmin;

-- grant apply on tag data_governance.costing_tags.environment to role sysadmin;
-- grant apply on tag data_governance.costing_tags.job_id to role sysadmin;
-- grant apply on tag data_governance.costing_tags.task_id to role sysadmin;

-- grant execute managed task on account to role sysadmin;
-- grant execute task on account to role sysadmin;

-- use role sysadmin;

-- select *
-- from snowflake.account_usage.tag_references
-- where tag_name = 'environment';

-- show tags in database data_governance;