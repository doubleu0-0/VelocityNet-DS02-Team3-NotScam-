
CREATE OR REPLACE PROCEDURE REPLACE_VALUES(
    TABLE_NAME STRING,
    COLUMN_NAME STRING,
    VALUE STRING,
    RVALUE STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    UPDATE_QUERY STRING;
    RESULT STRING;
    TABLE_EXISTS NUMBER;
    COLUMN_EXISTS NUMBER;
BEGIN
    -- Validate input parameters are not null
    IF (TABLE_NAME IS NULL OR COLUMN_NAME IS NULL OR VALUE IS NULL OR RVALUE IS NULL) THEN
        RETURN 'Error: Input parameters cannot be NULL';
    END IF;
    
    -- Check if table exists
    SELECT COUNT(*) 
    INTO :TABLE_EXISTS 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = UPPER(:TABLE_NAME);
    
    IF (TABLE_EXISTS = 0) THEN
        RETURN CONCAT('Error: Table ''', TABLE_NAME, ''' does not exist');
    END IF;
    
    -- Check if column exists
    SELECT COUNT(*) 
    INTO :COLUMN_EXISTS 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = UPPER(:TABLE_NAME) 
    AND COLUMN_NAME = UPPER(:COLUMN_NAME);
    
    IF (COLUMN_EXISTS = 0) THEN
        RETURN CONCAT('Error: Column ''', COLUMN_NAME, ''' does not exist in table ''', TABLE_NAME, '''');
    END IF;
    
    -- Construct the UPDATE query dynamically
    UPDATE_QUERY := CONCAT('UPDATE ', TABLE_NAME, ' SET ', COLUMN_NAME, ' = ', 
                          CASE 
                            WHEN TRY_TO_NUMBER(RVALUE) IS NOT NULL THEN RVALUE
                            ELSE CONCAT('''', RVALUE, '''')
                          END,
                          ' WHERE ', COLUMN_NAME, ' = ',
                          CASE 
                            WHEN TRY_TO_NUMBER(VALUE) IS NOT NULL THEN VALUE
                            ELSE CONCAT('''', VALUE, '''')
                          END);
    
    -- Execute the UPDATE query to replace values
    EXECUTE IMMEDIATE :UPDATE_QUERY;
    
    -- Return success message
    RETURN CONCAT('Table ', TABLE_NAME, ' updated successfully. Replaced ''', VALUE, ''' with ''', RVALUE, '''');
EXCEPTION
    WHEN OTHER THEN
        RETURN CONCAT('Error occurred: ', SQLERRM);
END;
$$;

CREATE TABLE SALESSO AS SELECT * FROM SALES_SPECIALOFFER;
SELECT * FROM SALESSO
CALL REPLACE_VALUES('SALESSO','MINQTY','0','-1')
SELECT * FROM SALESSO
DROP TABLE SALESSO
