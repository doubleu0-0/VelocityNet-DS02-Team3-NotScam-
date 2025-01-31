-- Jaden Khoo 
-- transfroming data to get aggregation
CREATE OR REPLACE TABLE SALES_PER_MONTH AS
SELECT 
TO_CHAR("OrderDate", 'YYYY-MM') || '-01' AS "OrderDateYearMonth",
SUM("SubTotal") AS "SubTotal",
SUM("TaxAmt") AS "TaxAmt",
SUM("Freight") AS "Freight",
SUM("TotalDue") AS "TotalDue"
FROM SALES_SALESORDERHEADER_CLEANED AS soh
LEFT JOIN SALES_SALESTERRITORY AS st ON soh."TerritoryID" = st."TerritoryID"
GROUP BY "OrderDateYearMonth";

CREATE OR REPLACE TABLE SALES_PER_MONTH AS
SELECT 
TO_DATE("OrderDateYearMonth", 'YYYY-MM-DD') AS "OrderDateYearMonth",
"SubTotal",
"TaxAmt",
"Freight",
"TotalDue",
CASE
    -- If the OrderDateYearMonth is the lowest, return the same value
    WHEN "OrderDateYearMonth" = '2011-05-01'
    THEN "OrderDateYearMonth"
    -- Otherwise, return one month before the OrderDateYearMonth
    ELSE DATEADD(MONTH, -1, TO_DATE("OrderDateYearMonth", 'YYYY-MM-DD'))
END AS "PreviousMonth"
FROM SALES_PER_MONTH;

CREATE OR REPLACE TABLE SALES_PER_MONTH AS
SELECT 
ppm1."OrderDateYearMonth",
ppm1."SubTotal",
ppm1."TaxAmt",
ppm1."Freight",
ppm1."TotalDue",
ppm1."PreviousMonth",
ppm2."TotalDue" AS "PastTotalDue"
FROM SALES_PER_MONTH AS ppm1
LEFT JOIN SALES_PER_MONTH AS ppm2 ON ppm1."PreviousMonth" = ppm2."OrderDateYearMonth";

CREATE OR REPLACE TABLE CHANGE_IN_SALES AS
SELECT 
"OrderDateYearMonth",
"SubTotal",
"TaxAmt",
"Freight",
"TotalDue",
"PreviousMonth",
"PastTotalDue",
"TotalDue" - "PastTotalDue" AS "ChangeInSales"
FROM SALES_PER_MONTH;

CREATE OR REPLACE TABLE CHANGE_IN_SALES AS
SELECT 
"OrderDateYearMonth",
TO_CHAR("OrderDateYearMonth", 'YYYY-MM') AS "YearMonthKey",
"SubTotal",
"TaxAmt",
"Freight",
"TotalDue",
"PreviousMonth",
"PastTotalDue",
"ChangeInSales",
CASE
    -- If ChangeInSales is positive
    WHEN "ChangeInSales" > 0 THEN
        'Sales ▲ by $' || TO_CHAR(ROUND(ABS("ChangeInSales"),2)) || ' this Month'
    -- If ChangeInSales is zero
    WHEN "ChangeInSales" = 0 THEN
        'No Previous Month'
    -- If ChangeInSales is negative
    WHEN "ChangeInSales" < 0 THEN
        'Sales ▼ by $' || TO_CHAR(ROUND(ABS("ChangeInSales"),2)) || ' this Month'
END AS "Description"
FROM CHANGE_IN_SALES;

-- temp table for data
CREATE OR REPLACE TABLE SALES_SALESPERSON_CLEANED_TEMP AS
SELECT 
"BusinessEntityID",
"TerritoryID",
"SalesQuota",
"Bonus",
"CommissionPct",
"SalesYTD",
"SalesLastYear"
FROM SALES_SALESPERSON_CLEANED;

CREATE OR REPLACE TABLE SALES_SALESTERRITORY_CLEANED_TEMP AS
SELECT 
"TerritoryID",
"Name", 
"CountryRegionCode",
"Group",
"SalesYTD",
"SalesLastYear"
FROM SALES_SALESTERRITORY_CLEANED;
