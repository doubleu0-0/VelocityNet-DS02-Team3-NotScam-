CREATE OR REPLACE PROCEDURE GET_TABLE_DETAILS(TABLE_NAME STRING)
RETURNS TABLE (
    COLUMN_NAME STRING,
    DATA_TYPE STRING,
    IS_NULLABLE STRING,
    ROW_COUNT NUMBER,
    DISTINCT_COUNT NUMBER,
    NULL_COUNT NUMBER,
    MODE_VALUE STRING
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
    SELECT CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME)
    INTO :full_table_name
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = UPPER(:TABLE_NAME)
    LIMIT 1;

    -- Build dynamic query for each column
    SELECT LISTAGG(
        'SELECT 
            ''' || COLUMN_NAME || ''' as COLUMN_NAME,
            ''' || DATA_TYPE || ''' as DATA_TYPE,
            ''' || IS_NULLABLE || ''' as IS_NULLABLE,
            COUNT(*) as ROW_COUNT,
            COUNT(DISTINCT ' || COLUMN_NAME || ') as DISTINCT_COUNT,
            COUNT(*) - COUNT(' || COLUMN_NAME || ') as NULL_COUNT,
            (SELECT CAST(' || COLUMN_NAME || ' AS STRING) 
             FROM (
                SELECT ' || COLUMN_NAME || ', COUNT(*) as freq
                FROM ' || :full_table_name || '
                WHERE ' || COLUMN_NAME || ' IS NOT NULL
                GROUP BY ' || COLUMN_NAME || '
                ORDER BY freq DESC
                LIMIT 1
             )) as MODE_VALUE
        FROM ' || :full_table_name,
        ' UNION ALL '
    )
    INTO :dynamic_query
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = UPPER(:TABLE_NAME);

    -- Execute the dynamic query
    res := (EXECUTE IMMEDIATE :dynamic_query);
    
    RETURN TABLE(res);
END;
$$;

CALL GET_TABLE_DETAILS('PERSON_ADDRESS')
