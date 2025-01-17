-- Call stored procedure to create the streams for each table
CALL create_streams_for_tables();

-- Create task to transform data, which will run after the TEAM3_TABLE_TRIGGERED_TASK gets the data from AWS
CREATE OR REPLACE TASK transform_data_task
    WAREHOUSE = TEAM3_WH    
    AFTER TEAM3_TABLE_TRIGGERED_TASK
    WHEN
        SYSTEM$STREAM_HAS_DATA('HUMANRESOURCES_DEPARTMENT_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('HUMANRESOURCES_EMPLOYEE_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PERSON_ADDRESS_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PERSON_STATEPROVINCE_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PRODUCTION_PRODUCT_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PRODUCT_CATEGORY_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PRODUCT_INVENTORY_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PURCHASING_PRODUCTVENDOR_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PURCHASING_PURCHASEORDERDETAIL_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PURCHASING_PURCHASEORDERHEADER_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_CREDITCARD_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_CURRENCY_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_CURRENCYRATE_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SALESORDERHEADER_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SALESPERSON_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SALESPERSONQUOTAHISTORY_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SALESREASON_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SALESORDERDETAIL_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SALESTERRITORY_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PRODUCT_SUBCATEGORY_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PURCHASING_SHIPMETHOD_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('PURCHASING_VENDOR_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SPECIALOFFER_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_CUSTOMER_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_STORE_STREAM')
        OR SYSTEM$STREAM_HAS_DATA('SALES_SPECIALOFFERPRODUCT_STREAM')
    AS
    BEGIN
        -- Cleaning for Sales_SalesTerritory
        CREATE OR REPLACE TABLE Sales_SalesTerritory_clean AS
        SELECT 
            TERRITORYID,
            NAME, 
            COUNTRYREGIONCODE,
            "Group",
            SALESYTD,
            SALESLASTYEAR,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM Sales_SalesTerritory;

        -- Cleaning for Sales_Customer
        CREATE OR REPLACE TABLE Sales_Customer_clean AS
        SELECT 
            CUSTOMERID,
            PERSONID,
            STOREID,
            TERRITORYID,
            ACCOUNTNUMBER,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM Sales_Customer;

        -- Cleaning for Sales_SalesOrderHeader
        CREATE OR REPLACE TABLE Sales_SalesOrderHeader_clean AS
        SELECT 
            SALESORDERID,
            ORDERDATE::DATE AS ORDERDATE,
            DUEDATE::DATE AS DUEDATE,
            SHIPDATE::DATE AS SHIPDATE,
            ONLINEORDERFLAG,
            SALESORDERNUMBER,
            PURCHASEORDERNUMBER,
            ACCOUNTNUMBER,
            CUSTOMERID,
            SALESPERSONID,
            TERRITORYID,
            BILLTOADDRESSID,
            SHIPMETHODID,
            SUBTOTAL,
            TAXAMT,
            FREIGHT,
            TOTALDUE,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM Sales_SalesOrderHeader;

        -- Cleaning for Sales_SalesOrderDetail
        CREATE OR REPLACE TABLE Sales_SalesOrderDetail_clean AS
        SELECT 
            SALESORDERID,
            SALESORDERDETAILID,
            CARRIERTRACKINGNUMBER,
            ORDERQTY,
            PRODUCTID,
            SPECIALOFFERID,
            UNITPRICE,
            UNITPRICEDISCOUNT,
            LINETOTAL,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM Sales_SalesOrderDetail;

        -- Cleaning for HumanResources_Department
        CREATE OR REPLACE TABLE HumanResources_Department_clean AS
        SELECT 
            DEPARTMENTID,
            NAME,
            GROUPNAME,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM HumanResources_Department;

        -- Cleaning for PURCHASING_PRODUCTVENDOR
        CREATE OR REPLACE TABLE PURCHASING_PRODUCTVENDOR_CLEANED AS 
        SELECT 
            PRODUCTID,
            BUSINESSENTITYID,
            AVERAGELEADTIME,
            STANDARDPRICE,
            LASTRECEIPTCOST,
            LASTRECEIPTDATE::DATE AS LASTRECEIPT_DATE,
            MINORDERQTY,
            MAXORDERQTY,
            COALESCE(ONORDERQTY, 0) AS ONORDER_QTY,
            UNITMEASURECODE,
            MODIFIEDDATE::DATE AS MODIFIED_DATE
        FROM PURCHASING_PRODUCTVENDOR
        WHERE MODIFIED_DATE IS NOT NULL AND LASTRECEIPT_DATE IS NOT NULL;

        -- Cleaning for PURCHASING_PURCHASEORDERDETAIL
        CREATE OR REPLACE TABLE PURCHASING_PURCHASEORDERDETAIL_CLEANED AS 
        SELECT
            PURCHASEORDERID,
            PURCHASEORDERDETAILID,
            DUEDATE::DATE AS DUE_DATE,
            ORDERQTY,
            PRODUCTID,
            UNITPRICE,
            LINETOTAL,
            RECEIVEDQTY,
            REJECTEDQTY, 
            STOCKEDQTY,
            MODIFIEDDATE::DATE AS MODIFIED_DATE
        FROM PURCHASING_PURCHASEORDERDETAIL
        WHERE MODIFIED_DATE IS NOT NULL AND DUE_DATE IS NOT NULL;

        -- Cleaning for PURCHASING_SHIPMETHOD
        CREATE OR REPLACE TABLE PURCHASING_SHIPMETHOD_CLEANED AS
        SELECT
            SHIPMETHODID,
            NAME,
            SHIPBASE,
            SHIPRATE,
            MODIFIEDDATE::DATE AS MODIFIED_DATE
        FROM PURCHASING_SHIPMETHOD
        WHERE MODIFIED_DATE IS NOT NULL;

        -- Cleaning for PURCHASING_PURCHASEORDERHEADER
        CREATE OR REPLACE TABLE PURCHASING_PURCHASEORDERHEADER_CLEANED AS 
        SELECT
            PURCHASEORDERID,
            REVISIONNUMBER,
            STATUS,
            EMPLOYEEID,
            VENDORID,
            SHIPMETHODID,
            ORDERDATE::DATE AS ORDER_DATE,
            SHIPDATE::DATE AS SHIP_DATE,
            SUBTOTAL,
            TAXAMT,
            FREIGHT,
            TOTALDUE,
            MODIFIEDDATE::DATE AS MODIFIED_DATE
        FROM PURCHASING_PURCHASEORDERHEADER
        WHERE SHIP_DATE IS NOT NULL AND ORDER_DATE IS NOT NULL AND MODIFIED_DATE IS NOT NULL;

        -- Cleaning for PURCHASING_VENDOR
        CREATE OR REPLACE TABLE PURCHASING_VENDOR_CLEANED AS 
        SELECT
            BUSINESSENTITYID,
            ACCOUNTNUMBER,
            NAME,
            CREDITRATING,
            PREFERREDVENDORSTATUS,
            ACTIVEFLAG,
            COALESCE(PURCHASINGWEBSERVICEURL, 'no_url') AS PURCHASINGWEBSERVICE_URL,
            MODIFIEDDATE::DATE AS MODIFIED_DATE
        FROM PURCHASING_VENDOR 
        WHERE MODIFIED_DATE IS NOT NULL;

        -- Cleaning for PRODUCT_SUBCATEGORY
        CREATE OR REPLACE TABLE PRODUCT_SUBCATEGORY_CLEAN AS 
        SELECT
            ProductSubcategoryID,
            ProductCategoryID,
            Name
        FROM PRODUCT_SUBCATEGORY
        WHERE NAME IS NOT NULL;

        -- Cleaning for PRODUCT_INVENTORY
        CREATE OR REPLACE TABLE PRODUCT_INVENTORY_CLEAN AS
        SELECT
            ProductID,
            LocationID,
            Shelf,
            Bin,
            Quantity,
            ModifiedDate::DATE AS Modified_date
        FROM PRODUCT_INVENTORY
        WHERE MODIFIEDDATE IS NOT NULL AND PRODUCTID IS NOT NULL;

        -- Cleaning for PRODUCT_CATEGORY
        CREATE OR REPLACE TABLE PRODUCT_CATEGORY_CLEAN AS
        SELECT
            ProductCategoryID,
            name
        FROM PRODUCT_CATEGORY
        WHERE NAME IS NOT NULL;

        -- Cleaning for SALES_STORE
        CREATE OR REPLACE TABLE Sales_Store_Cleaned AS
        SELECT
            BusinessEntityID,
            Name,
            SalesPersonID,
            AnnualSales,
            AnnualRevenue,
            BankName,
            BusinessType,
            YearOpened,
            Specialty,
            SquareFeet,
            Brands,
            Internet,
            NumberEmployees
        FROM SALES_STORE_PARSED
        WHERE NAME IS NOT NULL;

        -- Cleaning for PRODUCTION_PRODUCT
        CREATE OR REPLACE TABLE PRODUCTION_PRODUCT_CLEAN AS
        SELECT
            ProductID,
            Name,
            ProductNumber,
            MakeFlag,
            FinishedGoodsFlag,
            Color,
            SafetyStockLevel,
            ReorderPoint,
            StandardCost,
            ListPrice,
            Size,
            SizeUnitMeasureCode,
            WeightUnitMeasureCode,
            Weight,
            DaysToManufacture,
            ProductLine,
            Class,
            Style,
            ProductSubcategoryID,
            ProductModelID,
            SellStartDate::DATE AS SellStartDate,
            SellEndDate::DATE AS SellEndDate,
            DiscontinuedDate::DATE AS DiscontinuedDate
        FROM PRODUCTION_PRODUCT
        WHERE NAME IS NOT NULL 
            AND PRODUCTNUMBER IS NOT NULL 
            AND SELLSTARTDATE IS NOT NULL 
            AND LISTPRICE != 0 
            AND PRODUCTSUBCATEGORYID IS NOT NULL 
            AND PRODUCTMODELID IS NOT NULL;

        -- Cleaning for PERSON_STATEPROVINCE
        CREATE OR REPLACE TABLE PERSON_STATEPROVINCE_CLEANED AS
        SELECT 
            STATEPROVINCEID,
            STATEPROVINCECODE,
            COUNTRYREGIONCODE,
            ISONLYSTATEPROVINCEFLAG,
            NAME,
            TERRITORYID,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM PERSON_STATEPROVINCE;

        -- Cleaning for SALES_SPECIALOFFERPRODUCT
        CREATE OR REPLACE TABLE SALES_SPECIALOFFERPRODUCT_CLEANED AS
        SELECT 
            SPECIALOFFERID,
            PRODUCTID,
            ROWGUID,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM SALES_SPECIALOFFERPRODUCT;

        -- Cleaning for SALES_SPECIALOFFER
        CREATE OR REPLACE TABLE SALES_SPECIALOFFER_CLEANED AS
        SELECT 
            SPECIALOFFERID,
            DESCRIPTION,
            DISCOUNTPCT,
            TYPE,
            CATEGORY,
            STARTDATE,
            ENDDATE,
            MINQTY,
            MAXQTY,
            ROWGUID,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM SALES_SPECIALOFFER;

        -- Cleaning for PERSON_ADDRESS
        CREATE OR REPLACE TABLE PERSON_ADDRESS_CLEANED AS
        SELECT 
            ADDRESSID,
            ADDRESSLINE1,
            ADDRESSLINE2,
            CITY,
            STATEPROVINCEID,
            POSTALCODE,
            SPATIALLOCATION,
            ROWGUID,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM PERSON_ADDRESS;

        -- Cleaning for HUMANRESOURCES_EMPLOYEE
        CREATE OR REPLACE TABLE HUMANRESOURCES_EMPLOYEE_CLEANED AS
        SELECT 
            BUSINESSENTITYID,
            NATIONALIDNUMBER,
            LOGINID,
            ORGANIZATIONNODE,
            ORGANIZATIONLEVEL,
            JOBTITLE,
            BIRTHDATE,
            MARITALSTATUS,
            GENDER,
            HIREDATE,
            SALARIEDFLAG,
            VACATIONHOURS,
            SICKLEAVEHOURS,
            CURRENTFLAG,
            ROWGUID,
            MODIFIEDDATE::DATE AS MODIFIEDDATE
        FROM HUMANRESOURCES_EMPLOYEE;
    END;

-- Resume the task after the changes
ALTER TASK transform_data_task RESUME;
