-- Create a storage integration to connect external storage (S3 bucket)
CREATE OR REPLACE STORAGE INTEGRATION S3SNOWFLAKE_TEAM3_BACKUP
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::571600844113:role/team3-backup-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://team3-bucket-backup/team3database-backup/');

SHOW STORAGE INTEGRATIONS;
DESC INTEGRATION S3SNOWFLAKE_TEAM3_BACKUP;


CREATE OR REPLACE FILE FORMAT CSVFILEFORMATTEAM3BACKUP
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  NULL_IF = ('NULL');

  
-- Create the external stage object
CREATE OR REPLACE STAGE S3_STAGE_TEAM3_BACKUP   
  STORAGE_INTEGRATION = S3SNOWFLAKE_TEAM3_BACKUP
  URL = 's3://team3-bucket-backup/team3database-backup/'
  FILE_FORMAT = CSVFILEFORMATTEAM3BACKUP;

LIST @S3_STAGE_TEAM3_BACKUP;
DESCRIBE STAGE S3_STAGE_TEAM3_BACKUP;


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



-- Function to send sns notif
CREATE OR REPLACE NOTIFICATION INTEGRATION BACKUP_SNS_NOTIF_INT
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:571600844113:Team3BackupStatus'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::571600844113:role/team3-backup-role';
