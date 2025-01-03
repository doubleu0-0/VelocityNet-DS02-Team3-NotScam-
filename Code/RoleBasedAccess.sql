-- creating the roles
CREATE ROLE Team3_Tester;
CREATE ROLE Team3_Developer;
CREATE ROLE Team3_Master_Admin:

-- Grant Tester role read-only access
GRANT USAGE ON DATABASE TEAM3_DB TO ROLE Tester;
GRANT USAGE ON SCHEMA TEAM3_SCHEMA TO ROLE Tester;
GRANT USAGE ON TASK my_schema.my_task TO ROLE my_role;
GRANT USAGE ON STREAM my_schema.my_stream TO ROLE my_role;
GRANT USAGE ON PIPE my_schema.my_pipe TO ROLE my_role;
GRANT USAGE ON FILE FORMAT my_schema.my_file_format TO ROLE my_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TEAM3_SCHEMA TO ROLE Tester;
GRANT MONITOR ON WAREHOUSE TEAM3_WH TO ROLE Tester;


-- Grant Developer role privileges for development work like inserting and updating
GRANT USAGE ON DATABASE TEAM3_DB TO ROLE Developer;
GRANT USAGE ON SCHEMA TEAM3_SCHEMA TO ROLE Developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA TEAM3_SCHEMA TO ROLE Developer;
-- Allow creating new tables and views
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA TEAM3_SCHEMA TO ROLE Developer;
GRANT USAGE ON WAREHOUSE TEAM3_WH TO ROLE Developer;


-- Grant Master_Admin full administrative privileges
GRANT OWNERSHIP ON DATABASE TEAM3_DB TO ROLE Master_Admin;
GRANT OWNERSHIP ON SCHEMA TEAM3_SCHEMA TO ROLE Master_Admin;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA TEAM3_SCHEMA TO ROLE Master_Admin;
-- Allow creating new databases, schemas, and roles
GRANT CREATE DATABASE, CREATE SCHEMA, CREATE ROLE, CREATE USER TO ROLE Master_Admin;
-- Allow full control over the warehouse
GRANT OWNERSHIP ON WAREHOUSE TEAM3_WH TO ROLE Master_Admin;
-- Grant the ability to manage roles and users
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE Master_Admin;

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
