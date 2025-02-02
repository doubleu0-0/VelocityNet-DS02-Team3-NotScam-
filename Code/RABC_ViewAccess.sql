-- create new analyst user
use role accountadmin;
create or replace user TEAM3_VIEW
--populate with your own password
password = 'Team3View'
default_role = 'TEAM3_VIEW';


-- create analyst roles
create or replace role TEAM3_DB_VIEW;
-- grant the roles to the user we created above
grant role TEAM3_DB_VIEW to user TEAM3_VIEW;
-- to past users
grant role TEAM3_DB_VIEW to user FERRET;
grant role TEAM3_DB_VIEW to user FINCH;
grant role TEAM3_DB_VIEW to user FLAMINGO;
grant role TEAM3_DB_VIEW to user FALCON;


-- grant permission to warehouse
grant usage on warehouse TEAM3_WH to role TEAM3_DB_VIEW;

-- grant permissions to database
grant usage on database TEAM3_DB to role TEAM3_DB_VIEW;
-- grant schema to the roles
grant usage on schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;
grant usage on schema TEAM3_DB.PUBLIC to role TEAM3_DB_VIEW;
-- add future and past grants to table
grant select on all tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant select on all tables in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant select on future tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant select on future tables in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;
-- future grants for Dynamic Tables 
grant select on all dynamic tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant select on all dynamic tables in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant select on future dynamic tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant select on future dynamic tables in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;
--grant access to views 
grant select on all views in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant select on all views in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant select on future views in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant select on future views in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

-- grant usage to stage
grant usage on all stages in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on all stages in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant usage on future stages in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on future stages in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

-- grant usage to git repo
grant read on git repository TEAM3_DB.TEAM3_SCHEMA.VELOCITYNET to role TEAM3_DB_VIEW;
grant read on git repository TEAM3_DB.TEAM3_BACKUP.VELOCITYNET to role TEAM3_DB_VIEW;

-- grant usage to file formats
grant usage on all file formats in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on all file formats in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant usage on future file formats in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on future file formats in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

-- grant usage to pipes
grant operate on pipe TEAM3_DB.TEAM3_SCHEMA.TEAM3_PIPELINE to role TEAM3_DB_VIEW;

-- grant usage to streams
grant select on all Streams in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;

grant select on future Streams in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;

-- grant usage to tasks
grant operate on all tasks in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant operate on all tasks in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant operate on future tasks in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant operate on future tasks in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

-- grant usage to procedures
grant usage on all procedures in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on all procedures in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;

grant usage on future procedures in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_DB_VIEW;
grant usage on future procedures in schema TEAM3_DB.TEAM3_BACKUP to role TEAM3_DB_VIEW;
