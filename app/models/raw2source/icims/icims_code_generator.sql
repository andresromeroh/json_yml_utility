##  icims_raw_to_source is a code generator for ICIMS API download.  The execution is expected to fail and do not perform any data processing.
##  It reads the sample data from {{ raw_table_name }}.raw_data.  This macro works only for ICIMS datastream APIs.
##
## Parameters:
##    raw_table_name  -   Raw table name with JSON structure eg: hr_raw_qa.icims.submittal
##    dbt_source_name -   Source file name in models/source/icims_raw.yml eg: idw_package.dynamic_source('icims_raw', 'submittal')
##
## Output file: dbt_ingest/target/run/hrdm_ingest/models/raw2source/icims/icims_code_generator.sql
##
## Usage:
## 1. Enter into docker by executing the follwoing command in virtual environment
##         ./run.sh it
## 2. Run the icims_code_generator model within docker.
##         dbt run --project-dir /app/dbt_ingest/ --target dev -m icims_code_generator
## 3. The execution should fail and it is expected.  However, the code is generated in the following file.
##         cat dbt_ingest/target/run/hrdm_ingest/models/raw2source/icims/icims_code_generator.sql


{{ icims_raw_to_source('hr_raw_qa.icims.room', "'icims_raw', 'room'") }}
