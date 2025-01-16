USE ROLE ACCOUNTADMIN;

-- Creating the roles
CREATE ROLE Team3_Tester;
CREATE ROLE Team3_Developer;
CREATE ROLE Team3_Master_Admin;

-- Grant Tester role read-only access
-- Grant privileges on Database
GRANT USAGE, MONITOR 
    ON DATABASE TEAM3_DB 
    TO ROLE Team3_Tester;

-- Grant privileges on Schema
-- Main schema
GRANT USAGE, MONITOR 
    ON SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Tester;
GRANT SELECT 
    ON ALL TABLES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Tester;
GRANT SELECT 
    ON ALL VIEWS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Tester;
GRANT USAGE, READ 
    ON ALL STAGES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Tester;
GRANT USAGE 
    ON ALL PROCEDURES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Tester;

-- Allow full control over the warehouse
GRANT USAGE 
    ON WAREHOUSE TEAM3_WH 
    TO ROLE Team3_Tester;

-- Grant Developer role privileges for development work like inserting and updating
-- Grant privileges on Database
GRANT ALL 
    ON DATABASE TEAM3_DB 
    TO ROLE Team3_Developer;

-- Grant privileges on Schema
-- Main schema
GRANT ALL 
    ON SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL TABLES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL VIEWS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL STAGES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON INTEGRATION git_api_integration_team3 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON PIPE TEAM3_PIPELINE 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL STREAMS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL TASKS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Developer;

-- Backup schema
GRANT ALL 
    ON SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL TABLES IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL VIEWS IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL STAGES IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL STREAMS IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Developer;
GRANT ALL 
    ON ALL TASKS IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Developer;

-- Allow full control over the warehouse
GRANT ALL 
    ON WAREHOUSE TEAM3_WH 
    TO ROLE Team3_Developer;

-- Grant Master_Admin full administrative privileges
-- Grant privileges on Database
GRANT OWNERSHIP 
    ON DATABASE TEAM3_DB 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON DATABASE TEAM3_DB 
    TO ROLE Team3_Master_Admin;

-- Grant privileges on Schema
-- Main schema
GRANT OWNERSHIP 
    ON SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL TABLES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL VIEWS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL STAGES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON INTEGRATION git_api_integration_team3 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON PIPE TEAM3_PIPELINE 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL STREAMS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL TASKS IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL PROCEDURES IN SCHEMA TEAM3_DB.TEAM3_SCHEMA 
    TO ROLE Team3_Master_Admin;

-- Backup schema
GRANT OWNERSHIP 
    ON SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL TABLES IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL VIEWS IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL STAGES IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL STREAMS IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL TASKS IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON ALL PROCEDURES IN SCHEMA TEAM3_DB.TEAM3_BACKUP 
    TO ROLE Team3_Master_Admin;

-- Allow full control over the warehouse
GRANT OWNERSHIP 
    ON WAREHOUSE TEAM3_WH 
    TO ROLE Team3_Master_Admin;
GRANT ALL 
    ON WAREHOUSE TEAM3_WH 
    TO ROLE Team3_Master_Admin;

-- Grant the ability to manage roles and users
GRANT MANAGE GRANTS 
    ON ACCOUNT 
    TO ROLE Team3_Master_Admin;

-- Show grants for verification
SHOW GRANTS TO ROLE Team3_Master_Admin;

-- Grant the roles to the users
GRANT ROLE Team3_Tester TO USER FERRET;
GRANT ROLE Team3_Tester TO USER FINCH;
GRANT ROLE Team3_Tester TO USER FLAMINGO;
GRANT ROLE Team3_Tester TO USER FALCON;

GRANT ROLE Team3_Developer TO USER FERRET;
GRANT ROLE Team3_Developer TO USER FINCH;
GRANT ROLE Team3_Developer TO USER FLAMINGO;
GRANT ROLE Team3_Developer TO USER FALCON;

GRANT ROLE Team3_Master_Admin TO USER FERRET;
GRANT ROLE Team3_Master_Admin TO USER FINCH;
GRANT ROLE Team3_Master_Admin TO USER FLAMINGO;
GRANT ROLE Team3_Master_Admin TO USER FALCON;
