## This repo contains the work product for the Snowflake SQL/ETL Homework Assigment
**Sections:**
- Overview and Approach:  This describes the overall approach and assumptions
- File List and Description:  This provides a list of relevant project artifact files and their descriptions
- .sql Files and Description:  This provides a list of reuseable/composable Snowflake objects that may be used in other projects
- Procedure Notes: This provides additional description and explanation of the steps in our ingest_loan_monthly() procedure;

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

### .sql Files and Description

### Procedure Notes
