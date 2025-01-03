USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_BACKUP;
USE SCHEMA TEAM3_BACKUP;

CREATE OR REPLACE TASK clone_backup_task
  SCHEDULE = 'USING CRON 0 16 * * * UTC'  -- 12:00 AM SGT
  COMMENT = 'Task to call the clone_and_backup_all_tables procedure every day at 12:00 AM SGT'
  AS
  CALL clone_and_backup_all_tables('TEAM3_SCHEMA', 'TEAM3_BACKUP', 'S3_STAGE_TEAM3_BACKUP');


ALTER TASK clone_backup_task RESUME;