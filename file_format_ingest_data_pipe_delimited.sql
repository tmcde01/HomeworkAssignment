-- CREATE A FILE FORMAT FOR FILE INGESTION
-- drop file format raw_bronze.ingest_data_pipe_delimited;
create file format if not exists raw_bronze.ingest_data_pipe_delimited
    type = csv
    encoding = 'UTF8'
    field_delimiter = '|'
    skip_header = 1
    field_optionally_enclosed_by = '"'
    replace_invalid_characters = true
    null_if = ('NULL', 'null', 'None', '')
    empty_field_as_null = true
    trim_space = true
    compression = 'gzip';