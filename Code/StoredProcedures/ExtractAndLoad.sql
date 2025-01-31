-- By zachariah lo yiqi (s10257183D) 

--create a schema for viewing the datatypes
CREATE OR REPLACE FILE FORMAT CSVFILEFORMATTEAM3INFERENCER
    TYPE = CSV
    PARSE_HEADER=TRUE
    COMPRESSION = AUTO
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', '');

--placeholder table to store the metadata
CREATE OR REPLACE TABLE TEMP_TABLE(
    FILENAME STRING
);

-- Pipeline to automate data loading process when a file is loaded
CREATE OR REPLACE PIPE TEAM3_PIPELINE
    AUTO_INGEST = TRUE
    AS
    COPY INTO TEMP_TABLE FROM
    (SELECT DISTINCT METADATA$FILENAME FROM @S3_STAGE_LOGGER_TEAM3);

--display pipes created
SHOW PIPES;

--check if the temporary table is receiving data from the snowpipe
SELECT * FROM TEMP_TABLE;
--check if data is present in the two stages created
LIST @S3_STAGE_LOGGER_TEAM3;
LIST @S3_STAGE_TEAM3;

--debug the task if it fails or succeeds
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME DESC;
--check load history of a specific snowpipe
SELECT * FROM TABLE(INFORMATION_SCHEMA.LOAD_HISTORY_BY_PIPE('TEAM3_PIPELINE'));


-- create a stream to track changes in the data for the temp_table
CREATE OR REPLACE STREAM TEMP_TABLE_STREAM ON TABLE TEMP_TABLE;

--analyse changes made to the stream
SELECT * FROM TEMP_TABLE_STREAM;

--create a stored procedure to refresh the new stage logs table (called by the task TEAM3_TABLE_TRIGGERED_TASK)
CREATE OR REPLACE PROCEDURE TEAM3_STAGE_REFRESHER()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LIST @S3_STAGE_TEAM3;
    CREATE OR REPLACE TABLE TEAM3_STAGE_LOGS_NEW AS
    SELECT $1 AS FILENAME, $3 AS MD5_NEW
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    RETURN 'Suceeded';
END;
$$;


-- create a table to store stage metadata and refresh it in the task
CREATE OR REPLACE TABLE TEAM3_STAGE_LOGS_NEW AS
SELECT
$1 AS FILENAME,
$3 AS MD5_NEW
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM TEAM3_STAGE_LOGS_OLD;
SELECT * FROM TEAM3_STAGE_LOGS_NEW;

-- debug the stored procedure below
-- CALL TEAM3_TABLE_APPENDER();


--create a view to store a sql statement to find the tables in our current database so far
CREATE OR REPLACE VIEW TEAM3_SCHEMA_TABLES_VIEW AS
SELECT
TABLE_NAME,
LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'TEAM3_SCHEMA';

-- create another view to store the columns for easy filtering
CREATE OR REPLACE VIEW TEAM3_SCHEMA_COLUMNS_VIEW AS
SELECT
TABLE_NAME,
COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'TEAM3_SCHEMA'
ORDER BY ORDINAL_POSITION;


--empty table for debugging (this staging table is used to compare the current and old table data, whatever is new it will append the new records using a merge statement)
-- TRUNCATE TEAM3_STAGING_TABLE;



--This stored procedure takes all the current data present in temp_table, for each record it performs 2 checks, the first check is whether the data is in the database or not, if the table is inside, it will compair with the old stage logging and new logging file, if there is a change in the MD5, the file information, it will pull data from the stage into a staging table, then create a merge statement with string concatenation to append the additional rows into the current table. If the table is not inside, it will create a new table and copy the data from the stage into the table
CREATE OR REPLACE PROCEDURE TEAM3_TABLE_APPENDER()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    TABLE_EXISTS INT;
    FILE_NAME STRING;
    TABLE_NOT_PROCESSED INT;
    FILENAMES CURSOR FOR (SELECT DISTINCT tsl.FILENAME FROM TEAM3_STAGE_LOGS_NEW tsl JOIN TEMP_TABLE tt ON SPLIT_PART(SPLIT_PART(tt.FILENAME,'|', 1) || '.csv', '/', -1) =  SPLIT_PART(tsl.FILENAME, '/', -1));
    FILEPATH STRING;
    TABLENAME STRING;
    TABLE_EXEC_QUERY STRING;
    COPY_INTO_QUERY STRING;
    MERGE_QUERY STRING;
    ON_CONDITION STRING;
    KEY_COLUMNS STRING;
    UPDATE_COLUMNS STRING;
    ALL_COLUMNS STRING;
    ALL_VALUE_COLUMNS STRING;
BEGIN
    --iterate through the file names where filename is the file path of the stage
    FOR RECORD IN FILENAMES DO
        FILE_NAME := RECORD.FILENAME;

        --extract table name from filename
        FILEPATH := SPLIT_PART(FILE_NAME, '/', -1);
        TABLENAME := REGEXP_REPLACE(FILEPATH, '.csv', '');
        TABLENAME := UPPER(REGEXP_REPLACE(TABLENAME, '([a-z])([A-Z])', '\\1_\\2',2, 1));

        -- check if table inside database
        TABLE_EXISTS := (SELECT COUNT(*) FROM TEAM3_SCHEMA_TABLES_VIEW WHERE TABLE_NAME = :TABLENAME);
        --check if the table is in database and it has not been processed so its last altered date is earlier than today
        TABLE_NOT_PROCESSED := (SELECT COUNT(*) FROM (SELECT * FROM TEAM3_STAGE_LOGS_NEW n JOIN TEAM3_STAGE_LOGS_OLD o ON n.filename =                  o.filename WHERE UPPER(REGEXP_REPLACE(SPLIT_PART(n.FILENAME, '/', -1), '.csv', '')) = REPLACE(:TABLENAME, '_', '') AND MD5_OLD !=                MD5_NEW));
        
        --if table is not in database yet, create the table
        IF (TABLE_EXISTS = 0) THEN 
            TABLE_EXEC_QUERY := 'CREATE OR REPLACE TABLE ' || TABLENAME ||
                            ' USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM
                                TABLE(INFER_SCHEMA(
                                    LOCATION => ''@S3_STAGE_TEAM3'', 
                                    FILE_FORMAT => ''CSVFILEFORMATTEAM3INFERENCER'',
                                    FILES => (''' || FILEPATH || ''')
                                    )))';
                                
            EXECUTE IMMEDIATE TABLE_EXEC_QUERY;
        
            COPY_INTO_QUERY := 'COPY INTO ' || TABLENAME || ' 
                                    FROM @S3_STAGE_TEAM3
                                    FILE_FORMAT = CSVFILEFORMATTEAM3
                                    PATTERN = ''' || FILEPATH || '''';
        
            EXECUTE IMMEDIATE COPY_INTO_QUERY;
            CONTINUE;
            
        -- if the file exists and has not been modified (modified date in the processed table is earlier than the stage modified date)
        ELSEIF (TABLE_EXISTS > 0 AND TABLE_NOT_PROCESSED > 0) THEN

            ON_CONDITION := '';
            KEY_COLUMNS :='';
            UPDATE_COLUMNS :='';
            ALL_COLUMNS :='';
            ALL_VALUE_COLUMNS:='';
            MERGE_QUERY := '';
            
            --create the shell of the table
            TABLE_EXEC_QUERY := 'CREATE OR REPLACE TABLE TEAM3_STAGING_TABLE LIKE ' || TABLENAME;      
            EXECUTE IMMEDIATE TABLE_EXEC_QUERY;

            --copy data into the new tble
            COPY_INTO_QUERY := 'COPY INTO TEAM3_STAGING_TABLE
                                    FROM @S3_STAGE_TEAM3
                                    FILE_FORMAT = CSVFILEFORMATTEAM3
                                    PATTERN = ''' || FILEPATH || '''';

            EXECUTE IMMEDIATE COPY_INTO_QUERY;

            --build the merge query with the key columns of the table

            LET ALLS_RESULTS RESULTSET:= (SELECT COLUMN_NAME FROM TEAM3_SCHEMA_COLUMNS_VIEW WHERE TABLE_NAME = :TABLENAME);

            LET ALLS CURSOR FOR ALLS_RESULTS;

            FOR C IN ALLS DO
                ALL_VALUE_COLUMNS := :ALL_VALUE_COLUMNS || 's."' || C.COLUMN_NAME || '", ';
                ALL_COLUMNS := :ALL_COLUMNS || '"' || C.COLUMN_NAME || '", ';
            END FOR;   
            

            --get the key columns assuming they end with id

            LET KEYS_RESULTS RESULTSET := (SELECT COLUMN_NAME FROM TEAM3_SCHEMA_COLUMNS_VIEW WHERE TABLE_NAME = :TABLENAME 
                                          AND COLUMN_NAME ILIKE '%ID%');
            LET KEYS CURSOR FOR KEYS_RESULTS;

            FOR C IN KEYS DO
                ON_CONDITION := :ON_CONDITION || 't."' || C.COLUMN_NAME || '" = s."' || C.COLUMN_NAME || '" AND ';
                KEY_COLUMNS := :KEY_COLUMNS || '"' || C.COLUMN_NAME || '", ';
            END FOR;

            -- Remove the trailing 'AND' from the ON condition and the comma 
            ON_CONDITION := SUBSTRING(ON_CONDITION, 1, LENGTH(ON_CONDITION) - 5);
            KEY_COLUMNS := RTRIM(KEY_COLUMNS, ', ');
            ALL_COLUMNS := RTRIM(ALL_COLUMNS, ', ');
            ALL_VALUE_COLUMNS := RTRIM(ALL_VALUE_COLUMNS, ', ');

            -- Build the MERGE query
            MERGE_QUERY := 'MERGE INTO ' || TABLENAME || ' as t
                            USING TEAM3_STAGING_TABLE as s
                            ON ' || ON_CONDITION || '
                            WHEN MATCHED THEN
                                UPDATE SET ';

            -- Dynamically generate UPDATE columns (excluding the key columns)
            LET UPDATES_RESULTS RESULTSET := (SELECT COLUMN_NAME 
                                   FROM TEAM3_SCHEMA_COLUMNS_VIEW 
                                   WHERE TABLE_NAME = :TABLENAME 
                                    AND (COLUMN_NAME NOT ILIKE '%ID%' OR COLUMN_NAME ILIKE 'ROWGUID'));
            LET UPDATES CURSOR FOR UPDATES_RESULTS;

            FOR C IN UPDATES DO
                MERGE_QUERY := :MERGE_QUERY || '"' || C.COLUMN_NAME || '" = s."' || C.COLUMN_NAME || '", ';
                UPDATE_COLUMNS :=  :UPDATE_COLUMNS || '"' || C.COLUMN_NAME || '", ';
            END FOR;

            -- remove trailing comma
            UPDATE_COLUMNS := RTRIM(UPDATE_COLUMNS, ', ');

            -- Remove the trailing comma from the UPDATE statement
            MERGE_QUERY := RTRIM(MERGE_QUERY, ', ') || '
                            WHEN NOT MATCHED THEN
                                INSERT (' || ALL_COLUMNS || ')
                                VALUES (' || ALL_VALUE_COLUMNS || ')';

            -- Execute the MERGE query
            EXECUTE IMMEDIATE MERGE_QUERY;
            CONTINUE;

        ELSE
            CONTINUE;
        END IF;
    END FOR;
    CREATE OR REPLACE TABLE TEAM3_STAGE_LOGS_OLD AS SELECT FILENAME, MD5_NEW AS MD5_OLD FROM TEAM3_STAGE_LOGS_NEW;
    TRUNCATE TEMP_TABLE;
    RETURN 'Successfully made changes';
END;
$$;
    
