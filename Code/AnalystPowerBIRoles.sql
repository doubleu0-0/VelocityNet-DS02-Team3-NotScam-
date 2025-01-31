-- by Jaden Khoo (S10258662)
-- create new analyst user
use role accountadmin;
create or replace user TEAM3_ANALYST
--populate with your own password
password = 'Team3Analyst'
default_role = 'TEAM3_ANALYST_GLOBAL';


-- create analyst roles
create or replace role TEAM3_ANALYST_GLOBAL;
create or replace role TEAM3_ANALYST_NORTHAMERICA;
create or replace role TEAM3_ANALYST_EUROPE;
create or replace role TEAM3_ANALYST_PACIFIC;
-- grant the roles to the user we created above
grant role TEAM3_ANALYST_GLOBAL to user TEAM3_ANALYST;
grant role TEAM3_ANALYST_NORTHAMERICA to user TEAM3_ANALYST;
grant role TEAM3_ANALYST_EUROPE to user TEAM3_ANALYST;
grant role TEAM3_ANALYST_PACIFIC to user TEAM3_ANALYST;

-- grant roles to other users
grant role TEAM3_ANALYST_GLOBAL to user FERRET;
grant role TEAM3_ANALYST_NORTHAMERICA to user FERRET;
grant role TEAM3_ANALYST_EUROPE to user FERRET;
grant role TEAM3_ANALYST_PACIFIC to user FERRET;

grant role TEAM3_ANALYST_GLOBAL to user FINCH;
grant role TEAM3_ANALYST_NORTHAMERICA to user FINCH;
grant role TEAM3_ANALYST_EUROPE to user FINCH;
grant role TEAM3_ANALYST_PACIFIC to user FINCH;

grant role TEAM3_ANALYST_GLOBAL to user FLAMINGO;
grant role TEAM3_ANALYST_NORTHAMERICA to user FLAMINGO;
grant role TEAM3_ANALYST_EUROPE to user FLAMINGO;
grant role TEAM3_ANALYST_PACIFIC to user FLAMINGO;

grant role TEAM3_ANALYST_GLOBAL to user FALCON;
grant role TEAM3_ANALYST_NORTHAMERICA to user FALCON;
grant role TEAM3_ANALYST_EUROPE to user FALCON;
grant role TEAM3_ANALYST_PACIFIC to user FALCON;

-- grant permissions to database
grant usage on database TEAM3_DB to role TEAM3_ANALYST_GLOBAL;
grant usage on database TEAM3_DB to role TEAM3_ANALYST_NORTHAMERICA;
grant usage on database TEAM3_DB to role TEAM3_ANALYST_EUROPE;
grant usage on database TEAM3_DB to role TEAM3_ANALYST_PACIFIC;
-- grant schema to the roles
grant all on schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_GLOBAL;
grant all on schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_NORTHAMERICA;
grant all on schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_EUROPE;
grant all on schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_PACIFIC;
-- add future and past grants to table
grant all on all tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_GLOBAL;
grant all on all tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_NORTHAMERICA;
grant all on all tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_EUROPE;
grant all on all tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_PACIFIC;
grant all on future tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_GLOBAL;
grant all on future tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_NORTHAMERICA;
grant all on future tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_EUROPE;
grant all on future tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_PACIFIC;
-- future grants for Dynamic Tables 
grant all on future dynamic tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_GLOBAL;
grant all on future dynamic tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_NORTHAMERICA;
grant all on future dynamic tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_EUROPE;
grant all on future dynamic tables in schema TEAM3_DB.TEAM3_SCHEMA to role TEAM3_ANALYST_PACIFIC;
-- Create the TEAM3_POWERBI_WH
create or replace warehouse TEAM3_POWERBI_WH
warehouse_size = 'XSMALL'
auto_suspend = 60
initially_suspended = true
auto_resume = TRUE;
-- grant ownership of new warehouse and usage for past roles
GRANT OWNERSHIP ON WAREHOUSE TEAM3_POWERBI_WH TO ROLE Team3_Master_Admin;
GRANT ALL ON WAREHOUSE TEAM3_POWERBI_WH TO ROLE Team3_Master_Admin;
GRANT ALL ON WAREHOUSE TEAM3_POWERBI_WH TO ROLE TEAM3_DEVELOPER;
-- grant usage to the TEAM3_POWERBI_WH
grant usage on warehouse TEAM3_POWERBI_WH to role TEAM3_ANALYST_GLOBAL;
grant usage on warehouse TEAM3_POWERBI_WH to role TEAM3_ANALYST_NORTHAMERICA;
grant usage on warehouse TEAM3_POWERBI_WH to role TEAM3_ANALYST_EUROPE;
grant usage on warehouse TEAM3_POWERBI_WH to role TEAM3_ANALYST_PACIFIC;
