## This repo contains the work product for the Snowflake SQL/ETL Homework Assigment
****Note:****  

A short video link demonstrating the assignment work product and covering the topics in this README.md file is provided below.  It is recommended to view the video first, then refer to details below as needed:

Link:  

**Sections:**
- Overview and Approach:  This describes the overall approach and assumptions
- File List and Description:  This provides a list of relevant project artifact files and their descriptions
- .sql /.py Files and Description:  This provides a list of reuseable/composable Snowflake objects that may be used in other projects
- Procedure Notes: This provides additional description and explanation of the steps in our ingest_loan_monthly() procedures;

### Overview and Approach
 - Test-driven development is very often the desired approach:  We should obtain, create and use test data to validate our logic and our results (such as for idempotency).  In additional, data at scale is often too large for human comprehension, so we should anticipate and include identifiable edge cases
 -  We want to work as effeciently as possible.  Success here is defined as:  a small code base using repeatable and composable patterns; eliminating redundant processing by including data quality controls in the logical processing steps vs. after the fact error-checking; explicit coding style that is well commented and maintainable; efficient queries, comprehensive exception handling, logging and alerting; and document repositories.
 -  Importantly, we do not want a uni-directional process that can only the most recent files incrementally.  If we cannot perform a backfill or re-load without breaking downstream processes or performing signficant manual backout and correction then our design pattern is faulty.
 -  Lastly, a good employee figures out how to do what they are told without excessive supervision.  A better employee figures out how to do that they are told while adding additional value or developing new capabilities.  The best employee presents both options to their boss and then faithfully implements the final decision.

### File List and Explanation
  - Assignment.pdf:  This is the original assignment, reviewed and formated to help guide development work.  (Reorganization of requirements to support ordered workflow)
  - LOAN_MONTHLY_202601.csv:  This is a csv representing the first month's initial load for testing.  Columns and datatypes from the Assignment.pdf were used.  Additionally values were deliberately made difficult and non-uniform because often we cannot control vendor-supplied data.  A testing_note column has been added to track our test cases:  these should all read "original record"
  - LOAN_MONTHLY_202602.csv:  This is a second additional csv representing the second month's incremental load.  Records have been adjusted to reflect the test cases from the assignment instructions.  These are described in the testing_notes column, such as:  new records, monthly updates with our without changes, duplicate records with similar or different timestamps, secondary or additional records with updated timestamps.
  - LOAN_MONTHLY_202602.csv:  This is a third additional csv representing the third months's incremental load.  Records have been adjusted to reflect the data quality test cases from the assignment instructions.  These are also described in the testing notes column, such as: low row count, null check loan_id and null check reporting_month (concating the business key on these nulls is expected to fail), balance check and interest rate check < 0, interest rate check > 25
  - task_dag.md.  This is a screenshot of a snowflake task dag showing object dependencies.  Tasks are used to create the diagram, and could be used as wrappers to call procedures, but here are merely placeholders with additional notes. 

### .sql / .py Files and Description
 - database_object_creation.sql:  This file will stand up a generic database with our bronze, silver, and gold schemas.  An admin schema with control tables is included.  Also included are stages, file formats, and tasks, all in their relevant schema.  Comments are included with each object create statement.  It is portable and composable and can be used to create any similar db.  All that need to be done is "run all"
 - file_format_infer_schema_pipe_delimited.sql This is a composable file format that is portable for any db, just like the above file.  It to be used with the infer_schema() function to analyze files for content
 - file_format_ingest_data_pipe_delimited.sql: This is also a composable file format that is also portable for any db, just like the above file.  It to be used with the copy into function to ingest data
 - ingest_loan_monthly().sql: This is our base procedure that will run everyday.  See the Procedure Notes section below.
 - ingest_loan_monthly_backfill_or_reload(file_pattern_date varchar, remediation_type varchar).sql  This is our remdediation procedure that will backfill older files or remediate bad files.  See the Procedure Notes section below.
 - postgres_hc_connection_config.py:  The local server to PostgreSQL connector .py module, included as a module in postgres_files_to_snowflake.py below
 - snowflake_hc_connection_config.py: The local server to Snowflake connector .py module, included as a module in postgres_files_to_snowflake.py below
 - postgres_files_to_snowflake.py:  The main .py script that will transfer files from PostgreSQL to snowflake, intended to simulate a daily file drop from a vendor to an AWS External Stage.  (Here we use internal stages as a proxy)

### Procedure Notes
- ingest_loan_monthly();  This base procedure will scan our stage at pre-determined intervals for target files and ingest and process new loan_monthly files in accordance with our requirements.  Example: "call ingest_loan_monthly();"  It is explicitly coded and well-commented.  Note however the call for this procedure is automated via task and stream.  
- ingest_loan_monthly_backfill_or_reload(file_pattern_date varchar, remediation_type varchar); This remediation procedure will backfill older files or delete bad files and then reload updated versions, also in accordance with our requirements. Aguments include file_pattern date, which is a varchar representing dates outside the range of our regular target files in YYYY-MM format, and remediation_type which is a varchar representing whether to simply add additional older files ('backfill') or to remove bad files and reload them ('reload').  Example: "call ingest_loan_monthly_backfill_or_reload('2025-12', 'reload');"  It is also explicitly coded and well-commented.

### Bonus Items
 - Run book: 
   --  Step 1:  See the task_dag.md file in the "File List and Explanation" section for a flow diagram
   --  Step 2:  Using the file "database_object_creation.sql", run all  It will fail at line 72, just load the csv files from the git repo to the admin stage, then hit "Run all" again.  LIne 72:
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
      -- Step 3:  At this point you have everything except the tranform layer and logic.  This is discussed in the "Transform_Silver_Logic.sql" file.
 - Object dependency diagram.  See the task_dag.md file in the "File List and Explanation" section for a representative object dependency diagram.
 - Additional bonus item:  Load the files in this git into your local snowflake instance and see if it runs without error.  (Absent PosgreSQL, You will need to manually add files to the internal stages)
