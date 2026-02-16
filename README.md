## This repo contains the work product for the Snowflake SQL/ETL Homework Assigment
****Note:****  

A short video link demonstrating the assignment work product and covering the topics in this README.md file is provided below. 


### Homework Assignment Questions
- How do you prevent duplicate loading if the same file arrives twice?

  Please see CopyIntoProcedure.sql file for a complete review of the file ingest process.  For this specific question the following query is used.  It compares files in the gold layer
  to files in the stage:

  <img width="1083" height="346" alt="image" src="https://github.com/user-attachments/assets/4075ac6a-857c-4355-b7fa-a7e178d51dae" />

- How do you handle schema evolution if the file adds new columns?

  Create tables with schema evolution enabled.  Still, two approaches are suggested:  stop the process, trigger an alert, and alter the table manually for extra control and awareness; or script in the addition of the new column. (It is possible to make all the necessary DDL/DML adjustments downstream with dynamic scripting). Either way, for the new columns there will be null values for the old files, and new values for the files.  In the CopyIntoProcedure.sql file there is a check to ensure incoming files match the expected schema:

  <img width="1555" height="888" alt="image" src="https://github.com/user-attachments/assets/558204f0-ffa7-42a3-96e6-d596771093c4" />


- What would you do if COPY INTO partially loads a file and then fails?

  This depends on what my team and organization desires:  there could be a preference to skip the bad records and provide all available data, or to stop the process to investigate further.  This scenario is also included in the CopyIntoProcedure.sql.  Also please see the Successful Run with Skipped Files LOAN MONTHLY.txt log file.

  <img width="1463" height="515" alt="image" src="https://github.com/user-attachments/assets/fec100c4-8423-4472-ae9a-29c085c78c27" />

- What warehouse sizing / file sizing guidance would you give for performance?

  This is a huge topic, so I did some research.  Here is what I came up with:

  - Warehouse and file sizing depends heavily on the process step and use case. For COPY INTO operations, while larger warehouses can ingest massive files faster, every warehouse size doubles credit consumption, and there's a 60-second
    minimum billing window. For batch ingestion where latency isn't critical, I might use a Small warehouse and ingest slowly rather than paying for a Large warehouse that sits idle most of the time.
  - For file sizing, I'd compress files (gzip) and aim for 100-500 MB compressed files - large enough for Snowflake's automatic chunking and parallelization (which kicks in at 100MB+) but not so large they create stragglers.
  - For continuous micro-batch loading from external stages, Snowpipe with serverless compute is ideal - it auto-scales, charges per-second of actual processing, and uses AUTO_INGEST to trigger on file arrival.
  - For internal stages or scheduled workflows, serverless tasks follow similar cost principles.
  - Conversely, for user-facing queries against complex views, I'd size up the warehouse appropriately (Medium/Large) to ensure fast response times, since user experience matters and the warehouse can optimize complex queries better with
    more resources.
  - Key principles:

      (1) Start with X-Small/Small warehouses and scale only when performance testing shows clear benefit

      (2) Use serverless for sporadic, event-driven workloads; use dedicated warehouses for sustained processing

      (3) Enable aggressive auto-suspend (60 seconds) to avoid idle charges

      (4) Use multi-cluster autoscaling selectively for unpredictable concurrent workloads, not predictable batch jobs,

      (5) Monitor query performance and credit consumption to find the optimal balance.

### Project Artifact File List and Descriptions
  - Assignment.pdf:  This is the original assignment, reviewed and formated to help guide development work.  It shows a thought process behind the reorganization of requirements to support an ordered workflow.
  - LOAN_MONTHLY_202*.csv: These are all testing csv's I created to stand up the process and do testing.
  - *LOAN MONTHLY.txt:  These are all printouts from the logging table covering various testing scenarios.  The file name describes the scenario.
  - *.jpg.  These are all screenshots of different parts of the process.  The file name describes the process part.

### Main .sql Files and Description
  - CopyIntoProcedure.sql:  This is the raw anonymous block used for the Copy Into procedure in the RAW_BRONZE schema
  - TransformSilverLogic.sql: This is the CTE cascade used for creating the Loan Monthly Clean View (VW_LOAN_MONTHLY_CLEAN) in the TRANSFORM_SILVER schema
 
### Bonus Items
 - Run book: This is covered in the video.  Please see the link.
 - Object dependency diagrams. Please see the .jpg files in the Project Artifact File List and Descriptions sections for views of the database objects and task dags.
