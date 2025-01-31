-- By Tey Xue Cong (S10257059H)
USE ROLE TEAM3_MASTER_ADMIN;
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
EXECUTE AS CALLER
AS
$$
DECLARE
    table_name STRING;
    clone_sql STRING;
    copy_sql STRING;
    drop_sql STRING;
    table_exists INT;
    table_cursor CURSOR FOR
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'TEAM3_SCHEMA'
      AND table_type = 'BASE TABLE'
      AND table_name NOT LIKE 'AGG_%'
      AND table_name NOT LIKE 'DIM_%'
      AND table_name NOT LIKE 'FACT_%'
      AND table_name NOT LIKE 'VIEW_%';
BEGIN
    -- Iterate through all non-dynamic tables and non-view objects
    FOR record IN table_cursor DO
        table_name := record.TABLE_NAME;
        -- Ensure we are not backing up emptry table
        SELECT COUNT(*) INTO table_exists
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :backup_schema 
          AND TABLE_NAME = :table_name || '_BACKUP';

        IF (table_exists > 0) THEN
            drop_sql := 'DROP TABLE IF EXISTS "' || backup_schema || '"."' || table_name || '_BACKUP"'; -- Override past data
            EXECUTE IMMEDIATE drop_sql;
            
        END IF;
        -- Create table (Is ok to as we dropped all existing)
        clone_sql := 'CREATE TABLE ' || backup_schema || '.' || table_name || '_BACKUP CLONE ' || source_schema || '.' || table_name;
        EXECUTE IMMEDIATE clone_sql;
        -- Code to backup into the s3
        copy_sql := 'COPY INTO @' || stage_name || '/team3database-backup/' || table_name || '_backup.csv ' ||
                    'FROM ' || backup_schema || '.' || table_name || '_BACKUP ' ||
                    'OVERWRITE = TRUE FILE_FORMAT = (TYPE = ''CSV'' FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' ) SINGLE = TRUE';
        EXECUTE IMMEDIATE copy_sql;

    END FOR;

    RETURN 'All tables have been successfully cloned and backed up.';
END;
$$;

CALL clone_and_backup_all_tables('TEAM3_SCHEMA', 'TEAM3_BACKUP', 'S3_STAGE_TEAM3_BACKUP');


-- Run these 2 seperately
SELECT * FROM backup_tracking;

-- Hours since the previous successful backup
SELECT * FROM TIME_SINCE_LAST_BACKUP;
