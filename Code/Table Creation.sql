-- Cleaning Product_Subcategory_Clean
CREATE OR REPLACE TABLE PRODUCT_SUBCATEGORY_CLEAN AS 
SELECT
    ProductSubcategoryID,
    ProductCategoryID,
    Name
FROM PRODUCT_SUBCATEGORY
WHERE NAME IS NOT NULL;



-- Cleaning Product_Inventory_Clean
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



-- Cleaning Product_Category
CREATE OR REPLACE TABLE PRODUCT_CATEGORY_CLEAN AS
SELECT
    ProductCategoryID,
    name
FROM PRODUCT_CATEGORY
WHERE NAME IS NOT NULL;



-- Cleaning Sales_Store table
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



-- Clean Production_Product_Clean
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
WHERE NAME IS NOT NULL AND PRODUCTNUMBER IS NOT NULL AND SELLSTARTDATE IS NOT NULL AND LISTPRICE != 0 AND PRODUCTSUBCATEGORYID
IS NOT NULL AND PRODUCTMODELID IS NOT NULL;



-- NULLS in size unit measure code are either due to no size unit measure code
-- or due to sizes being in alphabetic representaion (S, M, L) or other ways such as shirt sizes
-- NULLS in SellEndDate and DiscontinuedDate are due to item not being discontinued or not having the sale ended
-- There are also nulls in weight, weightmeasurecode, class, and style
