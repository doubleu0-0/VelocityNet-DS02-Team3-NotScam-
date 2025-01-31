-- call stored procedure to create the streams for each table
CREATE OR REPLACE TASK CREATE_STREAMS_TASK
    WAREHOUSE = TEAM3_WH    
    AFTER TEAM3_TABLE_APPENDER_TASK
    AS
    BEGIN
        CALL create_streams_for_tables(); 
    END;

ALTER TASK CREATE_STREAMS_TASK RESUME;
