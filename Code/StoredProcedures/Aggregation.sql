USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_SCHEMA;
USE SCHEMA TEAM3_SCHEMA;

-- Aggregration stored procedure
CREATE OR REPLACE PROCEDURE GET_COLUMN_DETAILS(TABLE_NAME STRING, COLUMN_NAME STRING)
RETURNS TABLE (
    COLUMN_NAME STRING,
    COUNT NUMBER,
    MEAN NUMBER,
    STDDEV DOUBLE,
    MIN NUMBER,
    PERCENTILE_25 NUMBER,
    MEDIAN NUMBER,
    PERCENTILE_75 NUMBER,
    MAX NUMBER
)
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
    dynamic_query STRING;
    full_table_name STRING;
BEGIN
    -- Get fully qualified table name
    full_table_name := 'TEAM3_DB.TEAM3_SCHEMA.' || :TABLE_NAME;

    -- Build dynamic query to calculate statistics for the specified column
    dynamic_query := '
        SELECT ''' || :COLUMN_NAME || ''' as COLUMN_NAME,
            COUNT("' || :COLUMN_NAME || '") as COUNT,
            AVG("' || :COLUMN_NAME || '") as MEAN,
            STDDEV("' || :COLUMN_NAME || '") as STDDEV,
            MIN("' || :COLUMN_NAME || '") as MIN,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "' || :COLUMN_NAME || '") as PERCENTILE_25,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "' || :COLUMN_NAME || '") as MEDIAN,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "' || :COLUMN_NAME || '") as PERCENTILE_75,
            MAX("' || :COLUMN_NAME || '") as MAX
        FROM ' || :full_table_name || ' 
        WHERE "' || :COLUMN_NAME || '" IS NOT NULL';

    -- Execute the dynamic query
    res := (EXECUTE IMMEDIATE :dynamic_query);
    
    -- Return the result set
    RETURN TABLE(res);
END;
$$;



CALL GET_COLUMN_DETAILS('PURCHASING_PURCHASEORDERHEADER', 'SHIPMETHODID');
