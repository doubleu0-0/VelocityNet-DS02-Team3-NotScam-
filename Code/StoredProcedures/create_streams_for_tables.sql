-- Jaden Khoo (S10258662)
-- create stored proc for creating streams for each of the tables
CREATE OR REPLACE PROCEDURE create_streams_for_tables()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    -- Declare variables
    table_name STRING;
    sql_command STRING;
    -- define the cursor to get all the table names and allow looping through them
    -- some tables have to be filtered out and only the base tables are used
    c1 CURSOR FOR
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'TEAM3_SCHEMA'
          AND table_type = 'BASE TABLE'
          AND table_name IN ('HUMAN_RESOURCESDEPARTMENT','HUMAN_RESOURCESEMPLOYEE','PERSON_ADDRESS',
          'PERSON_PERSON','PERSON_STATEPROVINCE','PRODUCTION_PRODUCT','PRODUCTION_PRODUCTCATEGORY',
          'PRODUCTION_PRODUCTINVENTORY','PRODUCTION_PRODUCTSUBCATEGORY',
          'PURCHASING_PRODUCTVENDOR','PURCHASING_PURCHASEORDERDETAIL','PURCHASING_PURCHASEORDERHEADER',
          'SALES_CREDITCARD','SALES_CURRENCY','SALES_CURRENCYRATE','SALES_SALESORDERHEADER',
          'SALES_SALESPERSON','SALES_SALESPERSONQUOTAHISTORY','SALES_SALESREASON','SALES_SALESORDERDETAIL',
          'SALES_SALESTERRITORY','PURCHASING_SHIPMETHOD','PURCHASING_VENDOR',
          'SALES_SPECIALOFFER','SALES_CUSTOMER','SALES_STORE','SALES_SPECIALOFFERPRODUCT');

BEGIN
    -- Open the cursor to start fetching data
    FOR record IN c1 DO
        -- Get the table name from the cursor
        table_name := record.table_name;
        
        -- Construct the CREATE STREAM statement
        sql_command := 'CREATE OR REPLACE STREAM ' || table_name || '_stream ' || 
                          'ON TABLE TEAM3_SCHEMA.' || table_name || 
                          ' SHOW_INITIAL_ROWS = TRUE;';
        
        -- Execute the CREATE STREAM statement
        EXECUTE IMMEDIATE sql_command;
    END FOR;

    RETURN 'Created streams for each table';
END;
$$;
