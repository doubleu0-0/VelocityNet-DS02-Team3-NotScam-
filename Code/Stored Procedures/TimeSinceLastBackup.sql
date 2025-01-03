USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_BACKUP;
USE SCHEMA TEAM3_BACKUP;

-- This to track time since last backup
CREATE TABLE IF NOT EXISTS backup_tracking (
    backup_time TIMESTAMP,
    notes STRING
);


-- View time since last backup
CREATE OR REPLACE VIEW TIME_SINCE_LAST_BACKUP AS
SELECT 
    CURRENT_TIMESTAMP AS current_time,
    MAX(backup_time) AS last_backup_time,
    TIMESTAMPDIFF('hour', MAX(backup_time), CURRENT_TIMESTAMP) AS hours_since_last_backup
FROM backup_tracking
WHERE NOTES = 'Backup completed successfully';
