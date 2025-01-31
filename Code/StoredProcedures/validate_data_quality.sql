-- Jaden Khoo (S10258662)
CREATE OR REPLACE PROCEDURE validate_data_quality(staging_table STRING)
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    first_col_name STRING;
    missing_count INT;
    duplicate_count INT;
    sql_query_ID STRING;
    sql_query_N STRING;
    sql_query_D STRING;
    rsID RESULTSET;
    rsN RESULTSET;
    rsD RESULTSET;
BEGIN
    -- Find the name of the id column which is the first column
    sql_query_ID := 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ' ||
                    '''' || UPPER(staging_table) || '''' || ' AND ORDINAL_POSITION = 1';

    -- Execute the SQL and assign the result to rs, the result set
    rsID := (EXECUTE IMMEDIATE sql_query_ID);
    
    -- Use a cursor to get the data for id column name
    LET c1 CURSOR FOR rsID;
    OPEN c1;
    FETCH c1 INTO :first_col_name;
    CLOSE c1;
    -- Construct the SQL query to check for NULL values in the id column of the table
    sql_query_N := 'SELECT COUNT(*) FROM ' || staging_table || ' WHERE ' || :first_col_name || ' IS NULL';

    -- Execute the SQL and assign the result to rsN, the result set for null
    rsN := (EXECUTE IMMEDIATE sql_query_N);
    
    -- Use a cursor to get the data for the number of null values
    LET c2 CURSOR FOR rsN;
    OPEN c2;
    FETCH c2 INTO :missing_count;
    CLOSE c2;

    -- Return message
    BEGIN
        IF (:missing_count > 0) THEN
            RETURN 'There are ' || :missing_count || 'Null values in the table.';
        END IF;
    END;
    
    -- Construct the SQL query to check for duplicated values in the id column of the table
    sql_query_D := 'SELECT SUM(count_duplicated) FROM (SELECT COUNT(*) AS count_duplicated
                    FROM ' || staging_table || ' GROUP BY ' || :first_col_name || '
                    HAVING COUNT(*) > 1) AS duplicated_counts';

    -- Execute the SQL and assign the result to rsD, the result set for duplicates
    rsD := (EXECUTE IMMEDIATE sql_query_D);
    
    -- Use a cursor to get the data for the number of duplicated values
    LET c3 CURSOR FOR rsD;
    OPEN c3;
    FETCH c3 INTO :duplicate_count;
    CLOSE c3;
    -- Return message
    BEGIN
        IF (:duplicate_count > 0) THEN
            RETURN 'There are ' || :duplicate_count || 'Null values in the table.';
        END IF;
    END;
    
    -- If no issues were found, return validation passed message
    RETURN 'Data validation passed!';
END;
$$;
