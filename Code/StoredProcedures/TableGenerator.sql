--by zachariah loy yiqi
-- returns an IAM policy that grants a Snowflake SQS queue permission to subscribe to the SNS topic.
SELECT system$get_aws_sns_iam_policy('arn:aws:sns:us-west-2:851725251098:tem3_sns_topic_pipeline');

--create a schema for viewing the datatypes (difference is to include headers inside so the inferschema knows which column is which)
CREATE OR REPLACE FILE FORMAT CSVFILEFORMATTEAM3INFERENCER
    TYPE = CSV
    PARSE_HEADER=TRUE
    COMPRESSION = AUTO
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', '');

BEGIN TRANSACTION;
SELECT * 
FROM TABLE(
  INFER_SCHEMA(
    LOCATION => '@S3_STAGE_TEAM3',
    FILES => 'SalesCreditCard.csv',
    FILE_FORMAT => 'CSVFILEFORMATTEAM3INFERENCER'
  )
);
ROLLBACK;

DROP TABLE TEMP_TABLE;

--placeholder table to store the metadata
CREATE OR REPLACE TABLE TEMP_TABLE(
    FILENAME STRING
);



LIST @S3_STAGE_TEAM3;

-- Pipeline to automate data loading process when a file is loaded
CREATE OR REPLACE PIPE TEAM3_PIPELINE
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:851725251098:tem3_sns_topic_pipeline'
    AS
    COPY INTO TEMP_TABLE FROM
    (SELECT DISTINCT METADATA$FILENAME FROM @S3_STAGE_TEAM3)
    
SELECT *  FROM TEMP_TABLE;


-- create a stream to track changes in the data for the temp_table
CREATE OR REPLACE STREAM TEMP_TABLE_STREAM ON table TEMP_TABLE;

SELECT * FROM TEMP_TABLE_STREAM;

-- DROP PROCEDURE TEAM3_TABLE_CREATOR(STRING)

-- --stored proc to predefine and load data into the table
-- CREATE OR REPLACE PROCEDURE TEAM3_TABLE_CREATOR(FILENAME STRING)
--     RETURNS STRING
--     LANGUAGE SQL
--     EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     FILEPATH STRING;
--     TABLE_NAME STRING;
--     TABLE_EXEC_QUERY STRING;
--     COPY_INTO_QUERY STRING;
-- BEGIN

--     FILEPATH := SPLIT_PART(FILENAME, '/', -1);
    
--     TABLE_NAME := REGEXP_REPLACE(FILEPATH, '.csv', '');
--     TABLE_NAME := REGEXP_REPLACE(TABLE_NAME, '([a-z])([A-Z])', '\\1_\\2',2, 1);

--     TABLE_EXEC_QUERY := 'CREATE OR REPLACE TABLE ' || TABLE_NAME ||
--                         ' USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM
--                             TABLE(INFER_SCHEMA(
--                                 LOCATION => ''@S3_STAGE_TEAM3'', 
--                                 FILE_FORMAT => ''CSVFILEFORMATTEAM3INFERENCER'',
--                                 FILES => (''' || FILEPATH || ''')
--                                 )))';
                            
--     EXECUTE IMMEDIATE TABLE_EXEC_QUERY;

--     COPY_INTO_QUERY := 'COPY INTO ' || TABLE_NAME || ' 
--                             FROM @S3_STAGE_TEAM3
--                             FILE_FORMAT = CSVFILEFORMATTEAM3
--                             PATTERN = ''' || FILEPATH || '''';

--     EXECUTE IMMEDIATE :COPY_INTO_QUERY;
    
--     RETURN TABLE_NAME || ' created successfully!';  
-- END
-- $$;

-- DROP PROCEDURE FILE_LOOPER_FOR_TEMP_TABLE()

--create stored procedure to loop through new inserts in temp_table and call the stored proc
CREATE OR REPLACE PROCEDURE TEAM3_TABLE_CREATOR()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    FILE_NAME STRING;
    FILENAMES CURSOR FOR (SELECT DISTINCT FILENAME FROM TEMP_TABLE);
    FILEPATH STRING;
    TABLE_NAME STRING;
    TABLE_EXEC_QUERY STRING;
    COPY_INTO_QUERY STRING;
BEGIN
    FOR RECORD IN FILENAMES DO
        FILE_NAME := RECORD.FILENAME;

        FILEPATH := SPLIT_PART(:FILE_NAME, '/', -1);
    
        TABLE_NAME := REGEXP_REPLACE(FILEPATH, '.csv', '');
        TABLE_NAME := REGEXP_REPLACE(TABLE_NAME, '([a-z])([A-Z])', '\\1_\\2',2, 1);
    
        TABLE_EXEC_QUERY := 'CREATE OR REPLACE TABLE ' || TABLE_NAME ||
                            ' USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM
                                TABLE(INFER_SCHEMA(
                                    LOCATION => ''@S3_STAGE_TEAM3'', 
                                    FILE_FORMAT => ''CSVFILEFORMATTEAM3INFERENCER'',
                                    FILES => (''' || FILEPATH || ''')
                                    )))';
                                
        EXECUTE IMMEDIATE TABLE_EXEC_QUERY;
    
        COPY_INTO_QUERY := 'COPY INTO ' || TABLE_NAME || ' 
                                FROM @S3_STAGE_TEAM3
                                FILE_FORMAT = CSVFILEFORMATTEAM3
                                PATTERN = ''' || FILEPATH || '''';
    
        EXECUTE IMMEDIATE COPY_INTO_QUERY;
        
        DELETE FROM TEMP_TABLE WHERE FILENAME = :FILE_NAME; 
        
    END FOR;
    
    RETURN 'All files processed.';
END;
$$;

-- SELECT * FROM SALES_CURRENCY LIMIT 1

-- create task to automate the table creation process
CREATE OR REPLACE TASK TEAM3_TABLE_TRIGGERED_TASK  WAREHOUSE = TEAM3_WH
WHEN SYSTEM$STREAM_HAS_DATA('TEMP_TABLE_STREAM')
AS
CALL TEAM3_TABLE_CREATOR();
   
ALTER TASK TEAM3_TABLE_TRIGGERED_TASK SUSPEND;
ALTER TASK TEAM3_TABLE_TRIGGERED_TASK RESUME;


    
