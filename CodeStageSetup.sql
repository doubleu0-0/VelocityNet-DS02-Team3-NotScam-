-- set context and database for team 3
USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_SCHEMA;
USE SCHEMA TEAM3_SCHEMA;

-- create a stroage intergration object
CREATE OR REPLACE STORAGE INTEGRATION S3SNOWFLAKE_TEAM3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::851725251098:role/team3_snowflake_access_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://team3bucket/team3database/')

-- display storage intergration
SHOW STORAGE INTEGRATIONS;

--access the storage intergration to get the arn and id
DESC STORAGE INTEGRATION S3SNOWFLAKE_TEAM3;

-- 

CREATE OR REPLACE FILE FORMAT CSVFILEFORMATTEAM3
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  NULL_IF = ('NULL');


--create the external stage object
CREATE OR REPLACE STAGE S3_STAGE_TEAM3
  STORAGE_INTEGRATION = S3SNOWFLAKE_TEAM3
  URL = 's3://team3bucket/team3database/'
  FILE_FORMAT = CSVFILEFORMATTEAM3;

LIST @S3_STAGE_TEAM3;
DESCRIBE STAGE s3_stage_team3;










  
