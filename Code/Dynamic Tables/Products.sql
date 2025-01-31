create or replace dynamic table TEAM3_DB.TEAM3_SCHEMA.AGG_SALES_BY_SUBCATEGORY(
	"ProductSubcategoryID",
	"TotalOrderQty",
	"TotalSales",
	"TotalExpense",
	"TotalProfit",
	"AvgCostPerItem",
	"AvgSellingPricePerItem",
	"WeightedProfitMargin",
	"ProfitMarginCategory"
) target_lag = '1 hour' refresh_mode = INCREMENTAL initialize = ON_CREATE warehouse = TEAM3_WH
 as
SELECT 
    ps."ProductSubcategoryID",

    -- Aggregated Order Quantity
    SUM(s."OrderQty") AS "TotalOrderQty",
    
    -- Aggregated Total Sales
    SUM(s."LineTotal") AS "TotalSales",
    
    -- Aggregated Total Expense
    SUM(s."Expense") AS "TotalExpense",
    
    -- Aggregated Total Profit
    SUM(s."Profit") AS "TotalProfit",

    -- **Fix for AvgCostPerItem: Ensuring per-subcategory calculation**
    CASE 
        WHEN SUM(s."OrderQty") = 0 THEN NULL 
        ELSE SUM(s."Expense") / NULLIF(SUM(s."OrderQty"), 0)
    END AS "AvgCostPerItem",

    -- **Fix for AvgSellingPricePerItem**
    CASE 
        WHEN SUM(s."OrderQty") = 0 THEN NULL 
        ELSE SUM(s."LineTotal") / NULLIF(SUM(s."OrderQty"), 0)
    END AS "AvgSellingPricePerItem",

    -- Weighted Profit Margin
    CASE 
        WHEN SUM(s."LineTotal") = 0 THEN NULL 
        ELSE SUM(s."Profit") / NULLIF(SUM(s."LineTotal"), 0)
    END AS "WeightedProfitMargin",

    -- Profit Category Coloring
    CASE 
        WHEN SUM(s."Profit") / NULLIF(SUM(s."LineTotal"), 0) > 0 THEN 'Green'
        ELSE 'Red'
    END AS "ProfitMarginCategory"
    
FROM TEAM3_DB.TEAM3_SCHEMA.FACT_SALES_SALESORDERDETAIL s
LEFT JOIN TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCTION_PRODUCT p 
ON s."ProductID" = p."ProductID"
LEFT JOIN TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCTION_PRODUCTSUBCATEGORY ps
ON p."ProductSubcategoryID" = ps."ProductSubcategoryID"
GROUP BY 
    ps."ProductSubcategoryID";




CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCTION_PRODUCT(
	"ProductID",
	"Name",
	"ProductNumber",
	"MakeFlag",
	"FinishedGoodsFlag",
	"Color",
	"SafetyStockLevel",
	"ReorderPoint",
	"StandardCost",
	"ListPrice",
	"Size",
	"SizeUnitMeasureCode",
	"WeightUnitMeasureCode",
	"Weight",
	"DaysToManufacture",
	"ProductLine",
	"Class",
	"Style",
	"ProductSubcategoryID",
	"ProductModelID",
	"SellStartDate",
	"SellEndDate",
	"DiscontinuedDate"
) TARGET_LAG = 'DOWNSTREAM' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    "ProductID",
    "Name",
    "ProductNumber",
    "MakeFlag",
    "FinishedGoodsFlag",
    "Color",
    "SafetyStockLevel",
    "ReorderPoint",
    "StandardCost",
    "ListPrice",
    "Size",
    "SizeUnitMeasureCode",
    "WeightUnitMeasureCode",
    "Weight",
    "DaysToManufacture",
    "ProductLine",
    "Class",
    "Style",
    "ProductSubcategoryID",
    "ProductModelID",
    "SellStartDate",
    "SellEndDate",
    "DiscontinuedDate"
FROM TEAM3_DB.TEAM3_SCHEMA.PRODUCTION_PRODUCT_CLEANED;


CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCTION_PRODUCTSUBCATEGORY(
	"ProductSubcategoryID",
	"ProductCategoryID",
	"Name"
) TARGET_LAG = 'DOWNSTREAM' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    "ProductSubcategoryID",
    "ProductCategoryID",
    "Name"
FROM TEAM3_DB.TEAM3_SCHEMA.PRODUCTION_PRODUCTSUBCATEGORY_CLEANED;




CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCTION_PRODUCTCATEGORY(
	"ProductCategoryID",
	"Name"
) TARGET_LAG = 'DOWNSTREAM' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    "ProductCategoryID",
    "Name"
FROM TEAM3_DB.TEAM3_SCHEMA.PRODUCTION_PRODUCTCATEGORY_CLEANED;



CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.FACT_PURCHASING_PRODUCTVENDOR(
	"ProductID",
	"BusinessEntityID",
	"AverageLeadTime",
	"StandardPrice",
	"LastReceiptCost",
	"LastReceipt_Date",
	"MinOrderQty",
	"MaxOrderQty",
	"OnOrder_Qty",
	"UnitMeasureCode",
	"Modified_Date"
) TARGET_LAG = 'DOWNSTREAM' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    "ProductID",
    "BusinessEntityID",
    "AverageLeadTime",
    "StandardPrice",
    "LastReceiptCost",
    "LastReceipt_Date",
    "MinOrderQty",
    "MaxOrderQty",
    "OnOrder_Qty",
    "UnitMeasureCode",
    "Modified_Date"
FROM TEAM3_DB.TEAM3_SCHEMA.PURCHASING_PRODUCTVENDOR_CLEANED;




CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.FACT_SALES_SALESORDERDETAIL(
	"SalesOrderID",
	"SalesOrderDetailID",
	"CarrierTrackingNumber",
	"OrderQty",
	"ProductID",
	"SpecialOfferID",
	"UnitPrice",
	"UnitPriceDiscount",
	"LineTotal",
	"Expense",
	"Profit",
	"ProfitMargin",
	"ModifiedDate"
) TARGET_LAG = 'DOWNSTREAM' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    s."SalesOrderID",
    s."SalesOrderDetailID",
    s."CarrierTrackingNumber",
    s."OrderQty",
    s."ProductID",
    s."SpecialOfferID",
    s."UnitPrice",
    s."UnitPriceDiscount",
    s."LineTotal",
    (s."OrderQty" * COALESCE(p."StandardCost", 0)) AS "Expense",
    (s."LineTotal" - (s."OrderQty" * COALESCE(p."StandardCost", 0))) AS "Profit",
    -- Profit Margin Calculation (Profit / LineTotal)
    CASE 
        WHEN s."LineTotal" = 0 THEN NULL 
        ELSE (s."LineTotal" - (s."OrderQty" * COALESCE(p."StandardCost", 0))) / s."LineTotal"
    END AS "ProfitMargin",
    s."ModifiedDate"
FROM TEAM3_DB.TEAM3_SCHEMA.SALES_SALESORDERDETAIL_CLEANED s
LEFT JOIN TEAM3_DB.TEAM3_SCHEMA.DIM_PRODUCTION_PRODUCT p 
ON s."ProductID" = p."ProductID";



CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.FACT_SALES_SALESORDERHEADER(
	"SalesOrderID",
	"OrderDate",
	"YearMonthKey",
	"Month",
	"MonthNumber",
	"Year",
	"DueDate",
	"ShipDate",
	"OnlineOrderFlag",
	"SalesOrderNumber",
	"PurchaseOrderNumber",
	"AccountNumber",
	"CustomerID",
	"SalesPersonID",
	"TerritoryID",
	"BillToAddressID",
	"CreditCardID",
	"CreditCardApprovalCode",
	"ShipMethodID",
	"SubTotal",
	"TaxAmt",
	"Freight",
	"TotalDue",
	"ModifiedDate"
) TARGET_LAG = 'DOWNSTREAM' 
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    s."SalesOrderID",
    s."OrderDate",
    TO_CHAR(s."OrderDate", 'YYYY-MM') AS "YearMonthKey",
    TO_CHAR(s."OrderDate", 'Mon') AS "Month",
    EXTRACT(MONTH FROM s."OrderDate") AS "MonthNumber",
    TO_CHAR(s."OrderDate", 'YYYY') AS "Year",
    s."DueDate",
    s."ShipDate",
    s."OnlineOrderFlag",
    s."SalesOrderNumber",
    s."PurchaseOrderNumber",
    s."AccountNumber",
    s."CustomerID",
    s."SalesPersonID",
    s."TerritoryID",
    s."BillToAddressID",
    s."CreditCardID",
    s."CreditCardApprovalCode",
    s."ShipMethodID",
    s."SubTotal",
    s."TaxAmt",
    s."Freight",
    s."TotalDue",
    s."ModifiedDate"
FROM TEAM3_DB.TEAM3_SCHEMA.SALES_SALESORDERHEADER_CLEANED s;


CREATE OR REPLACE DYNAMIC TABLE TEAM3_DB.TEAM3_SCHEMA.DIM_SALES_SALESTERRITORY2(
	"TerritoryID",
	"Name",
	"CountryRegionCode",
	"Group",
	"SalesYTD",
	"SalesLastYear",
	"ModifiedDate"
) TARGET_LAG = 'DOWNSTREAM'  
  REFRESH_MODE = INCREMENTAL 
  INITIALIZE = ON_CREATE 
  WAREHOUSE = TEAM3_WH
AS
SELECT 
    t."TerritoryID",
    t."Name",
    t."CountryRegionCode",
    t."Group",
    t."SalesYTD",
    t."SalesLastYear",
    t."ModifiedDate"
FROM TEAM3_DB.TEAM3_SCHEMA.SALES_SALESTERRITORY_STAGING t;
