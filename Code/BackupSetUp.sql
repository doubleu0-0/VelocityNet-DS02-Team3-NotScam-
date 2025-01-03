USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_BACKUP;
USE SCHEMA TEAM3_BACKUP;

-- Create a storage integration to connect external storage (S3 bucket)
-- DO NOT RERUN THIS; will need to chagne the external ID for trust policy in IAM role in AWS
/*
CREATE OR REPLACE STORAGE INTEGRATION S3SNOWFLAKE_TEAM3_BACKUP
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::571600844113:role/team3-backup-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://team3-bucket-backup/team3database-backup/');
*/


-- Function to send sns notif
-- DO NOT RERUN THIS; will need to chagne the external ID for trust policy in IAM role in AWS
/*
CREATE OR REPLACE NOTIFICATION INTEGRATION BACKUP_SNS_NOTIF_INT
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:571600844113:Team3BackupStatus'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::571600844113:role/team3-backup-role';
*/
  
-- Create the external stage object
CREATE OR REPLACE STAGE S3_STAGE_TEAM3_BACKUP   
  STORAGE_INTEGRATION = S3SNOWFLAKE_TEAM3_BACKUP
  URL = 's3://team3-bucket-backup/team3database-backup/'
  FILE_FORMAT = CSVFILEFORMATTEAM3BACKUP;

