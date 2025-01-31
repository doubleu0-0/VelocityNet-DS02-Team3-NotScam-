-- Joshua
/*-------------------------------------------------
--CREATE DATE DIMENSION
-------------------------------------------------*/

/* SET THE DATE RANGE TO BUILD DATE DIMENSION */
SET MIN_DATE = (SELECT MIN("OrderDate") FROM SALES_SALESORDERHEADER_CLEANED);
SET MAX_DATE = (SELECT MAX("OrderDate") FROM SALES_SALESORDERHEADER_CLEANED);
SET DAYS = (SELECT $MAX_DATE - $MIN_DATE);

CREATE OR REPLACE TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_DATE_OFFERS
(
    DATE_ID INT,
    DATE DATE,
    YEAR STRING, 
    MONTH SMALLINT, 
    MONTH_NAME STRING, 
    DAY_OF_MONTH SMALLINT, 
    DAY_OF_WEEK SMALLINT, 
    WEEKDAY STRING,
    WEEK_OF_YEAR SMALLINT, 
    DAY_OF_YEAR SMALLINT,
    WEEKEND_FLAG BOOLEAN
)
AS
WITH DATES AS
(
    SELECT DATEADD(DAY, SEQ4(), $MIN_DATE) AS MY_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => $DAYS)) -- NUMBER OF DAYS AFTER REFERENCE DATE
)
SELECT
    TO_NUMBER(REPLACE(TO_VARCHAR(MY_DATE), '-')) AS DATE_ID,
    MY_DATE,
    YEAR(MY_DATE) AS YEAR,
    MONTH(MY_DATE) AS MONTH,
    MONTHNAME(MY_DATE) AS MONTH_NAME,
    DAY(MY_DATE) AS DAY_OF_MONTH,
    DAYOFWEEK(MY_DATE) AS DAY_OF_WEEK,
    DAYNAME(MY_DATE) AS WEEKDAY,
    WEEKOFYEAR(MY_DATE) AS WEEK_OF_YEAR,
    DAYOFYEAR(MY_DATE) AS DAY_OF_YEAR,
    CASE WHEN DAYOFWEEK(MY_DATE) IN (0, 6) THEN TRUE ELSE FALSE END AS WEEKEND_FLAG
FROM DATES;
/*-------------------------------------------------
--CREATE DYNAMIC SPECIAL OFFER DIMENSION
-------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_SPECIAL_OFFER_OFFERS
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
AS
SELECT
    "SpecialOfferID",
    CONCAT('Offer_', TO_VARCHAR("SpecialOfferID")) AS SPECIAL_OFFER_NAME,
    "StartDate",
    "EndDate",
    "DiscountPct",
    "MaxQty",
    "MinQty",
    "Description",
    "Type"
FROM TEAM3_DB.TEAM3_SCHEMA.SALES_SPECIALOFFER_CLEANED;

/*-------------------------------------------------
--CREATE DYNAMIC PRODUCT DIMENSION
-------------------------------------------------*/
CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCT_OFFERS
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
AS
SELECT
    P."ProductID",
    P."Name" AS ProductName,
    C."Name" AS ProductCategoryName,
    SC."Name" AS ProductSubCategoryName
FROM 
    PRODUCTION_PRODUCTSUBCATEGORY_CLEANED SC
INNER JOIN 
    PRODUCTION_PRODUCT_CLEANED P
    ON P."ProductSubcategoryID" = SC."ProductSubcategoryID"
INNER JOIN 
    PRODUCTION_PRODUCTCATEGORY_CLEANED C
    ON SC."ProductCategoryID" = C."ProductCategoryID";

/*-------------------------------------------------
--CREATE DYNAMIC SALES ORDER DIMENSION
-------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_SALES_ORDER_OFFERS
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
AS
SELECT
    "SalesOrderID",
    "CustomerID",
    "SalesOrderNumber",
    "OrderDate",
    "ShipDate",
    "TotalDue",
    "Freight",
    "SubTotal",
    "TaxAmt"
    FROM TEAM3_DB.TEAM3_SCHEMA.SALES_SALESORDERHEADER_CLEANED;

/*-------------------------------------------------
--CREATE DYNAMIC SPECIAL OFFER PRODUCT DIMENSION
-------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_SPECIAL_OFFER_PRODUCT_OFFERS
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
AS
SELECT
    CONCAT("ProductID", '_', "SpecialOfferID") AS PRODUCT_OFFER_KEY,
    "SpecialOfferID",
    "ProductID"
FROM TEAM3_DB.TEAM3_SCHEMA.SALES_SPECIALOFFERPRODUCT_CLEANED;

/*-------------------------------------------------
--CREATE DYNAMIC FACT OFFERS TABLE
-------------------------------------------------*/

CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.FACT_OFFERS
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
AS
SELECT
    SO."SalesOrderID", 
    P."ProductID",
    SOF."SpecialOfferID",
    D."DATE_ID", 
    SO."CustomerID", 
    SOI."OrderQty", 
    SOI."LineTotal" AS SALES_AMOUNT, 
    SOI."UnitPriceDiscount" AS DISCOUNT_AMOUNT, 
    SO."TotalDue", 
    SO."TaxAmt" AS TAX_AMOUNT, 
    SO."Freight", 
    SO."SubTotal" AS SUBTOTAL, 
    CONCAT(P."ProductID", '_', SOF."SpecialOfferID") AS PRODUCT_OFFER_KEY  
FROM 
    TEAM3_DB.TEAM3_SCHEMA.SALES_SALESORDERHEADER_CLEANED SO
INNER JOIN 
    TEAM3_DB.TEAM3_SCHEMA.SALES_SALESORDERDETAIL_CLEANED SOI 
    ON SO."SalesOrderID" = SOI."SalesOrderID"
INNER JOIN 
    TEAM3_DB.TEAM3_SCHEMA.DIM_DATE_OFFERS D 
    ON D."DATE" = SO."OrderDate" 
LEFT JOIN 
    TEAM3_DB.TEAM3_SCHEMA.DIM_SPECIAL_OFFER_PRODUCT_OFFERS SOF
    ON CONCAT(SOI."ProductID", '_', SOI."SpecialOfferID") = SOF."PRODUCT_OFFER_KEY"
LEFT JOIN 
    TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCT_OFFERS P
    ON P."ProductID" = SOI."ProductID";
