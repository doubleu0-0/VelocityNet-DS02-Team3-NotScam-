USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_BACKUP;
USE SCHEMA TEAM3_BACKUP;


-- Stored procedure to clone and backup all tables
CREATE OR REPLACE PROCEDURE clone_and_backup_all_tables(
    source_schema STRING,
    backup_schema STRING,
    stage_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    -- Declare variables
    table_name STRING;
    clone_sql STRING;
    copy_sql STRING;
    drop_sql STRING;
    table_exists INT;
    table_cursor CURSOR FOR SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'TEAM3_SCHEMA'AND TABLE_TYPE = 'BASE TABLE';
    
BEGIN
    -- Open the cursor to loop through the tables in the source schema
    FOR record IN table_cursor DO
        -- Get the table name from the cursor record
        table_name := record.TABLE_NAME;

        -- Check if the backup table already exists
        SELECT COUNT(*) INTO table_exists
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :backup_schema 
          AND TABLE_NAME = :table_name || '_BACKUP';

        -- If the backup table exists, drop it
        IF (:table_exists > 0) THEN
            -- Drop the backup table
            drop_sql := 'DROP TABLE IF EXISTS "' || backup_schema || '"."' || table_name || '_BACKUP"';
            EXECUTE IMMEDIATE drop_sql;
        END IF;

        -- Clone the table from source schema to backup schema
        clone_sql := 'CREATE TABLE ' || backup_schema || '.' || table_name || '_BACKUP CLONE ' || source_schema || '.' || table_name;
        EXECUTE IMMEDIATE clone_sql;

        -- Create the COPY INTO SQL to export the cloned tables to S3
        copy_sql := 'COPY INTO @' || stage_name || '/team3database-backup/' || table_name || '_backup/ ' ||
                   'FROM ' || backup_schema || '.' || table_name || '_BACKUP ' ||
                   'OVERWRITE = TRUE FILE_FORMAT = (TYPE = ''CSV'' FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' ) SINGLE = TRUE';
        EXECUTE IMMEDIATE copy_sql;

    END FOR;

    -- Record the backup completion time
    EXECUTE IMMEDIATE 'INSERT INTO backup_tracking (backup_time, notes) VALUES (CURRENT_TIMESTAMP, ''Backup completed successfully'')';
    
    -- Return success message
    RETURN 'All tables have been successfully cloned and backed up.';
END;
$$;

CALL clone_and_backup_all_tables('TEAM3_SCHEMA', 'TEAM3_BACKUP', 'S3_STAGE_TEAM3_BACKUP');


-- Run these 2 seperately
SELECT * FROM backup_tracking;

-- Hours since the previous successful backup
SELECT * FROM TIME_SINCE_LAST_BACKUP;
