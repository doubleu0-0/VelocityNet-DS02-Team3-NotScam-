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
