## This repo contains the work product for the Snowflake SQL/ETL Homework Assigment
****Note:****  

A short video link demonstrating the assignment work product and covering the topics in this README.md file is provided below.  It is recommended to view the video first, then refer to details below as needed:

Link:  

### Homework Assignment Questions
- How do you prevent duplicate loading if the same file arrives twice?

  The first step of our copy into process checks files in the stage against existing filenames in the target_gold layer.  Ingest only happens if the file is not found.
  
- How do you handle schema evolution if the file adds new columns?

  Tables are created with schema evolution allowing for this.  Still, two approaches are suggested:  stop the process, trigger an alert, and alter the table manually for extra control and awareness; or script in the addition of the new
  column. (It is possible to make all the necessary DDL/DML adjustments downstream with dynamic scripting). Either way, for the new columns there will be null values for the old files, and new values for the files.  In the approach here I
  include a check in the copy into procedure to ensure the new file conforms to the expected file schema.  If not, an exception is triggered.  I prefer control in order to prevent unanticipated downstream effects -- auto-adding columns in
  production could be risky.

- What would you do if COPY INTO partially loads a file and then fails?

  This depends on what my team and organization desires:  there could be a preference to skip the bad records and provide all available data, or to stop the process to investigate further.  Again, my preference is further investigation to
  ensure data quality and integrity via analysis and testing.  Here I included an exception in my copy into procedure that compares rows encountered to rows loaded, and if there is not a 1:1 match I trigger the exception.  Normally I
  would then save the data in the landing table to a failover table and reset the process for the next day.  Then I would analyze the data in the failover table.

- What warehouse sizing / file sizing guidance would you give for performance?

  This is a huge topic, so I did some research.  Here is what I came up with:

  -- Warehouse and file sizing depends heavily on the process step and use case. For COPY INTO operations, while larger warehouses can ingest massive files faster, every warehouse size doubles credit consumption, and there's a 60-second
     minimum billing window. For batch ingestion where latency isn't critical, I might use a Small warehouse and ingest slowly rather than paying for a Large warehouse that sits idle most of the time.

  -- For file sizing, I'd compress files (gzip) and aim for 100-500 MB compressed files - large enough for Snowflake's automatic chunking and parallelization (which kicks in at 100MB+) but not so large they create stragglers.

  -- For continuous micro-batch loading from external stages, Snowpipe with serverless compute is ideal - it auto-scales, charges per-second of actual processing, and uses AUTO_INGEST to trigger on file arrival.

  -- For internal stages or scheduled workflows, serverless tasks follow similar cost principles.

  -- Conversely, for user-facing queries against complex views, I'd size up the warehouse appropriately (Medium/Large) to ensure fast response times, since user experience matters and the warehouse can optimize complex queries better with
     more resources.

  -- Key principles: (1) Start with X-Small/Small warehouses and scale only when performance testing shows clear benefit, (2) Use serverless for sporadic, event-driven workloads; use dedicated warehouses for sustained processing, (3) Enable aggressive auto-suspend (60 seconds) to avoid idle charges, (4) Use multi-cluster autoscaling selectively for unpredictable concurrent workloads, not predictable batch jobs, (5) Monitor query performance and credit consumption to find the optimal balance."

### Project Artifact File List and Descriptions
  - Assignment.pdf:  This is the original assignment, reviewed and formated to help guide development work.  It shows a thought process behind the reorganization of requirements to support an ordered workflow.
  - LOAN_MONTHLY_202601.csv:  This is a csv representing the first month's initial load for testing.  Columns and datatypes from the Assignment.pdf were used.  Additionally values were deliberately made difficult and non-uniform because often we cannot control vendor-supplied data.  A testing_note column has been added to track our test cases:  these should all read "original record"
  - LOAN_MONTHLY_202602.csv:  This is a second additional csv representing the second month's incremental load.  Records have been adjusted to reflect the test cases from the assignment instructions.  These are described in the testing_notes column, such as:  new records, monthly updates with our without changes, duplicate records with similar or different timestamps, and secondary or additional records with updated timestamps.
  - LOAN_MONTHLY_202603.csv:  This is a third additional csv representing the third months's incremental load.  Records have been adjusted to reflect the data quality test cases from the assignment instructions.  These are also described in the testing notes column, such as: low row count; null check on loan_id, null check on reporting_month, and null check on loan_id + reporting_month (concating nulls to generate the business key is expected to fail); balance check for a negative value; interest rate check < 0; and interest rate check > 25
  - GZ Versions of the csv files: For each of the csv's, you want to use the .gz version with the Snowflake project.
  - database_explorer.md.  This is a screenshot of the HOMEWORK_ASSIGNMENT database showing object locations dependencies.
  - task_dag.md.  This is a screenshot of a snowflake task dag showing procedure dependencies.

### Main .sql Files and Description
  - database_object_creation.sql:  This file will stand up a generic database with our bronze, silver, and gold schemas.  An admin schema with control tables is included.  Also included are stages, file formats, and tasks, all in their relevant schema.  Comments are included with each object create statement.  Because it uses scripting vs. static typing, it is portable and composable and can be used to create any similar db.  
  - loan_monthly_copy_into_bronze.sql: This is our copy into raw/bronze procedure.  It also uses dynamic scripting so it is also composable and portable.  (Note it is included in database_object_creation.sql as part of our run book) 
  - loan_monthly_merge_into_gold.sql:  This is our merge into target/gold procedure.  Again, dynamically scripted. (Note also it is included in database_object_creation.sql as part of our run book)
  - Note:  Because we use a view for the transforms as the transform/silver layer, and views are stored queries, we don't have a procedure for this.  The view creation logic is found in the database_object_creation.sql file.  But also see the transform_silver_logic.sql file in the section below, which is provided for review and discussion.

### Additional .sql / .py Files and Description
  - transform_silver_logic.sql:  This is a separate worksheet showing a CTE cascade used for the data transform logic needed to clean up the records.  It can be reviewed individually for simplicity.
  - file_format_infer_schema_pipe_delimited.sql This is a composable file format that is portable for any db, just like the above file.  It to be used with the infer_schema() function to analyze file metadata.  Provided as an example
  - file_format_ingest_data_pipe_delimited.sql: This is also a composable file format that is also portable for any db, just like the above file.  It to be used with the copy into function to ingest data.  Also provided as an example
  - postgres_hc_connection_config.py:  The local server to PostgreSQL connector .py module, included as a module in postgres_files_to_snowflake.py below
  - snowflake_hc_connection_config.py: The local server to Snowflake connector .py module, included as a module in postgres_files_to_snowflake.py below
  - postgres_files_to_snowflake.py:  The main .py script that will transfer files from PostgreSQL to snowflake, intended to simulate a daily file drop from a vendor to an AWS External Stage.  (Here we use internal stages as a proxy)
 
### Bonus Items
 - Run book: 
   -  Step 1: Using the file "database_object_creation.sql", run all.
   -  Step 2: The scripts will stop at line 69 with the error message "STOP HERE:  You will need to upload the test file 'LOAN_MONTHLY_202601.csv.gz' to the stage at admin_schema.testing_files".  Do that.  
   -  Step 3: Hit "Run all" again. The object creation commands are constructed to prevent overwriting, i.e., "CREATE OBJECT IF NOT EXISTS"
   -  Step 4: You have now created all the necessary objects and procedures
   -  Step 5: Uncomment the last section "CHECK PROCESS OR CALL PROCEDURES". Follow the instructions to load files, run the process, and check the output.
   -  Step 6: (Optional)  If you like, follow the instructions for creating / restarting the task dag.  Then monitor the ingest/ETL procedure as you like.  Again however, you will need to load files to the raw_bronze.daily_files stage and then ensure it gets refreshed to trigger the process. If the process has run previously you will need to add new files with new filenames, because previously ingested files will be skipped. 
 - Object dependency diagrams.  See the database_explorer.md and task_dag.md files in the "Project Artifact File List and Descriptions" section.
