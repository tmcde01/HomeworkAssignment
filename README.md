## This repo contains the work product for the Snowflake SQL/ETL Homework Assigment
**Sections:**
- Overview and Approach
- File List and Description
- .sql Files and Description
- Procedure Notes

### File list and explanation
  - Assignment.pdf:  This is the original assignment, reviewed and formated to help guide development work.  (Reorganization of requirements to support ordered workflow)
  - LOAN_MONTHLY_202601.csv:  This is a csv representing the first month's initial load for testing.  Columns and datatypes from the Assignment.pdf were used.  Additionally values were deliberately made difficult and non-uniform because often we cannot control vendor-supplied data.  A testing_note column has been added to track our test cases:  these should all read "original record"
  - LOAN_MONTHLY_202602.csv:  This is a second additional csv representing the second month's incremental load.  Records have been adjusted to reflect the test cases from the assignment instructions.  These are described in the testing_notes column, such as:  new records, monthly updates with our without changes, duplicate records with similar or different timestamps, secondary or additional records with updated timestamps.
  - LOAN_MONTHLY_202602.csv:  This is a third additional csv representing the third months's incremental load.  Records have been adjusted to reflect the data quality test cases from the assignment instructions.  These are also described in the testing notes column, such as: low row count, null check loan_id and null check reporting_month (concating the business key on these nulls is expected to fail), balance check and interest rate check < 0, interest rate check > 25
