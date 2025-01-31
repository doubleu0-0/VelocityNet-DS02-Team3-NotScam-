-- By Jaden Khoo (S10258662)
-- fact table for FACT_SALES_ORDERHEADER
create or replace dynamic table FACT_SALES_ORDERHEADER
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
as
SELECT 
"SalesOrderID",
"OrderDate",
TO_CHAR("OrderDate", 'YYYY-MM') AS "YearMonthKey",
TO_CHAR("OrderDate", 'Mon') AS "Month",
EXTRACT(MONTH FROM "OrderDate") AS "MonthNumber",
TO_CHAR("OrderDate", 'YYYY') AS "Year",
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
"ShipMethodID",
"SubTotal",
"TaxAmt",
"Freight",
"TotalDue",
FROM SALES_SALESORDERHEADER_CLEANED;

-- dynamic table for DIM_PERSON_PERSON
create or replace dynamic table DIM_PERSON_PERSON
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
as
SELECT 
"BusinessEntityID",
"PersonType",
"NameStyle",
"Title",
"FirstName",
"MiddleName",
"LastName",
"Suffix",
"EmailPromotion",
"AdditionalContactInfo"
FROM PERSON_PERSON_CLEANED;


-- dynamic table for DIM_SALES_SALESPERSON
create or replace dynamic table DIM_SALES_SALESPERSON
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
as
SELECT 
"BusinessEntityID",
"TerritoryID",
"SalesQuota",
"Bonus",
"CommissionPct",
"SalesYTD",
"SalesLastYear"
FROM SALES_SALESPERSON_CLEANED_TEMP;

-- dynamic table for DIM_SALES_SALESTERRITORY
create or replace dynamic table DIM_SALES_SALESTERRITORY
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
as
SELECT 
"TerritoryID",
"Name", 
"CountryRegionCode",
"Group",
"SalesYTD",
"SalesLastYear"
FROM SALES_SALESTERRITORY_CLEANED_TEMP;



-- dynamic table for AGG_CHANGE_IN_SALES
create or replace dynamic table AGG_CHANGE_IN_SALES
target_lag = 'DOWNSTREAM'
warehouse = TEAM3_WH
refresh_mode = incremental
initialize = on_create
as
SELECT 
"OrderDateYearMonth",
"YearMonthKey",
"SubTotal",
"TaxAmt",
"Freight",
"TotalDue",
"PreviousMonth",
"PastTotalDue",
"ChangeInSales",
"Description"
FROM CHANGE_IN_SALES;
