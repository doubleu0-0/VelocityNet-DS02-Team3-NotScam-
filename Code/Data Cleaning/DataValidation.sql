-- Joshua data validation
--Check for null values
SELECT *
FROM PERSON_ADDRESS_CLEANED
WHERE ADDRESSID IS NULL OR CITY IS NULL OR STATEPROVINCEID IS NULL OR POSTALCODE IS NULL
OR SPATIALLOCATION IS NULL OR MODIFIEDDATE IS NULL OR ADDRESS IS NULL

--Check for null values
SELECT *
FROM HUMANRESOURCES_EMPLOYEE_CLEANED
WHERE BUSINESSENTITYID IS NULL OR NATIONALIDNUMBER IS NULL OR LOGINID IS NULL 
OR ORGANIZATIONNODE IS NULL OR ORGANIZATIONLEVEL IS NULL OR JOBTITLE IS NULL 
OR BIRTHDATE IS NULL OR MARITALSTATUS IS NULL OR GENDER IS NULL
OR HIREDATE IS NULL OR SALARIEDFLAG IS NULL OR VACATIONHOURS IS NULL
OR SICKLEAVEHOURS IS NULL OR CURRENTFLAG IS NULL OR MODIFIEDDATE IS NULL

--Check for null values
SELECT *
FROM PERSON_STATEPROVINCE_CLEANED
WHERE STATEPROVINCEID IS NULL OR STATEPROVINCECODE IS NULL OR COUNTRYREGIONCODE IS NULL
OR ISONLYSTATEPROVINCEFLAG IS NULL OR NAME IS NULL OR TERRITORYID IS NULL
OR MODIFIEDDATE IS NULL

--Check for null values
SELECT *
FROM SALES_SPECIALOFFERPRODUCT_CLEANED
WHERE SPECIALOFFERID IS NULL OR PRODUCTID IS NULL OR MODIFIEDDATE IS NULL

--Check for null values
SELECT *
FROM SALES_SPECIALOFFER_CLEANED
WHERE SPECIALOFFERID IS NULL OR DESCRIPTION IS NULL OR DISCOUNTPCT IS NULL
OR TYPE IS NULL OR CATEGORY IS NULL OR STARTDATE IS NULL OR ENDDATE IS NULL
OR MINQTY IS NULL OR MAXQTY IS NULL OR MODIFIEDDATE IS NULL



--Check for duplicate ids
SELECT ADDRESSID, COUNT(*) AS duplicate_count
FROM PERSON_ADDRESS_CLEANED
GROUP BY ADDRESSID
HAVING COUNT(*) > 1;

--Check for duplicate ids
SELECT BUSINESSENTITYID, COUNT(*) AS duplicate_count
FROM HUMANRESOURCES_EMPLOYEE_CLEANED
GROUP BY BUSINESSENTITYID
HAVING COUNT(*) > 1;

--Check for duplicate ids
SELECT STATEPROVINCEID, COUNT(*) AS duplicate_count
FROM PERSON_STATEPROVINCE_CLEANED
GROUP BY STATEPROVINCEID
HAVING COUNT(*) > 1;

--Check for duplicate ids
SELECT SPECIALOFFERID, PRODUCTID, COUNT(*) AS duplicate_count
FROM SALES_SPECIALOFFERPRODUCT_CLEANED
GROUP BY SPECIALOFFERID, PRODUCTID
HAVING COUNT(*) > 1;

--Check for duplicate ids
SELECT SPECIALOFFERID, COUNT(*) AS duplicate_count
FROM SALES_SPECIALOFFER_CLEANED
GROUP BY SPECIALOFFERID
HAVING COUNT(*) > 1;




--zachariah loy yiqi data validation
--data validation
SELECT *
FROM PURCHASING_PRODUCTVENDOR_CLEANED
WHERE PRODUCTID IS NULL OR BUSINESSENTITYID IS NULL OR AVERAGELEADTIME IS NULL OR STANDARDPRICE IS NULL
OR LASTRECEIPTCOST IS NULL OR LASTRECEIPT_DATE IS NULL OR MINORDERQTY IS NULL OR MAXORDERQTY IS NULL OR ONORDER_QTY IS NULL OR UNITMEASURECODE IS NULL  OR MODIFIED_DATE IS NULL

SELECT *
FROM PURCHASING_PURCHASEORDERDETAIL_CLEANED
WHERE PURCHASEORDERID IS NULL OR PURCHASEORDERDETAILID IS NULL OR DUE_DATE IS NULL 
OR ORDERQTY IS NULL OR PRODUCTID IS NULL OR UNITPRICE IS NULL 
OR LINETOTAL IS NULL OR RECEIVEDQTY IS NULL OR REJECTEDQTY IS NULL
OR STOCKEDQTY IS NULL OR MODIFIED_DATE IS NULL

SELECT *
FROM PURCHASING_VENDOR_CLEANED
WHERE BUSINESSENTITYID IS NULL OR ACCOUNTNUMBER IS NULL OR NAME IS NULL OR CREDITRATING IS NULL OR PREFERREDVENDORSTATUS IS NULL
OR ACTIVEFLAG IS NULL OR  PURCHASINGWEBSERVICE_URL IS NULL OR MODIFIED_DATE IS NULL

SELECT *
FROM PURCHASING_PURCHASEORDERHEADER_CLEANED
WHERE PURCHASEORDERID IS NULL OR REVISIONNUMBER IS NULL OR STATUS IS NULL
OR EMPLOYEEID IS NULL OR VENDORID IS NULL OR SHIPMETHODID IS NULL OR ORDER_DATE IS NULL
OR SHIP_DATE IS NULL OR SUBTOTAL IS NULL OR TAXAMT IS NULL OR FREIGHT IS NULL OR TOTALDUE IS NULL OR MODIFIED_DATE IS NULL



--Check for duplicate ids
SELECT PURCHASEORDERID, COUNT(*) AS duplicate_count
FROM PURCHASING_PURCHASEORDERHEADER_CLEANED
GROUP BY PURCHASEORDERID
HAVING COUNT(*) > 1;


SELECT PURCHASEORDERDETAILID, PURCHASEORDERID, COUNT(*) AS duplicate_count
FROM PURCHASING_PURCHASEORDERDETAIL_CLEANED
GROUP BY PURCHASEORDERDETAILID, PURCHASEORDERID
HAVING COUNT(*) > 1;


SELECT BUSINESSENTITYID, COUNT(*) AS duplicate_count
FROM PURCHASING_VENDOR_CLEANED
GROUP BY BUSINESSENTITYID
HAVING COUNT(*) > 1;


SELECT PRODUCTID, BUSINESSENTITYID, COUNT(*) AS duplicate_count
FROM PURCHASING_PRODUCTVENDOR_CLEANED
GROUP BY PRODUCTID, BUSINESSENTITYID
HAVING COUNT(*) > 1;

-- check rows
SELECT 'PURCHASING_PRODUCTVENDOR' AS table_name, COUNT(*) AS row_count
FROM PURCHASING_PRODUCTVENDOR
UNION
SELECT 'PURCHASING_PRODUCTVENDOR_CLEANED', COUNT(*)
FROM PURCHASING_PRODUCTVENDOR_CLEANED;

SELECT 'PURCHASING_VENDOR' AS table_name, COUNT(*) AS row_count
FROM PURCHASING_VENDOR
UNION
SELECT 'PURCHASING_VENDOR_CLEANED', COUNT(*)
FROM PURCHASING_VENDOR_CLEANED;

SELECT 'PURCHASING_PURCHASEORDERDETAIL' AS table_name, COUNT(*) AS row_count
FROM PURCHASING_PURCHASEORDERDETAIL
UNION
SELECT 'PURCHASING_PURCHASEORDERDETAIL_CLEANED', COUNT(*)
FROM PURCHASING_PURCHASEORDERDETAIL_CLEANED;

SELECT 'PURCHASING_PURCHASEORDERHEADER' AS table_name, COUNT(*) AS row_count
FROM PURCHASING_PURCHASEORDERHEADER
UNION
SELECT 'PURCHASING_PURCHASEORDERHEADER_CLEANED', COUNT(*)
FROM PURCHASING_PURCHASEORDERHEADER_CLEANED;

-- Jaden Data validation
-- Sales_SalesTerritory_clean
Select * from Sales_SalesTerritory_clean;
-- check for duplicates
SELECT TERRITORYID, COUNT(*) AS duplicate_count
FROM Sales_SalesTerritory_clean
GROUP BY TERRITORYID
HAVING COUNT(*) > 1;
-- check for null values in ID columns
SELECT *
FROM Sales_SalesTerritory_clean
WHERE TERRITORYID IS NULL;
-- check the number of rows afterwards
SELECT COUNT(TERRITORYID) FROM Sales_SalesTerritory_clean;

-- Sales_Customer_clean
Select * from Sales_Customer_clean;
-- check for duplicates
SELECT CUSTOMERID, COUNT(*) AS duplicate_count
FROM Sales_Customer_clean
GROUP BY CUSTOMERID
HAVING COUNT(*) > 1;
-- check for null values in ID columns
SELECT *
FROM Sales_Customer_clean
WHERE CUSTOMERID IS NULL;
-- check the number of rows afterwards
SELECT COUNT(CUSTOMERID) FROM Sales_Customer_clean;

-- Sales_SalesOrderHeader_clean
Select * from Sales_SalesOrderHeader_clean;
-- check for duplicates
SELECT SALESORDERID, COUNT(*) AS duplicate_count
FROM Sales_SalesOrderHeader_clean
GROUP BY SALESORDERID
HAVING COUNT(*) > 1;
-- check for null values in ID columns
SELECT *
FROM Sales_SalesOrderHeader_clean
WHERE SALESORDERID IS NULL;
-- check if all the customer ID are in the customer table
SELECT CUSTOMERID
FROM Sales_SalesOrderHeader_clean soh
WHERE soh.CUSTOMERID NOT IN (SELECT c.CUSTOMERID FROM Sales_Customer_clean C);
-- check the number of rows afterwards
SELECT COUNT(SALESORDERID) FROM Sales_SalesOrderHeader_clean;

-- Sales_SalesOrderDetail_clean
Select * from Sales_SalesOrderDetail_clean;
-- check for duplicates
SELECT SALESORDERDETAILID, COUNT(*) AS duplicate_count
FROM Sales_SalesOrderDetail_clean
GROUP BY SALESORDERDETAILID
HAVING COUNT(*) > 1;
-- check for null values in ID columns
SELECT *
FROM Sales_SalesOrderDetail_clean
WHERE SALESORDERDETAILID IS NULL;
-- check if all the sales order ID are in the sales order header table
SELECT SALESORDERID
FROM Sales_SalesOrderDetail_clean sod
WHERE sod.SALESORDERID NOT IN (SELECT soh.SALESORDERID FROM Sales_SalesOrderHeader_clean soh);
-- check the number of rows afterwards
SELECT COUNT(SALESORDERDETAILID) FROM Sales_SalesOrderDetail_clean;

-- HumanResources_Department_clean
Select * from HumanResources_Department_clean;
-- check for duplicates
SELECT DEPARTMENTID, COUNT(*) AS duplicate_count
FROM HumanResources_Department_clean
GROUP BY DEPARTMENTID
HAVING COUNT(*) > 1;
-- check for null values in ID columns
SELECT *
FROM HumanResources_Department_clean
WHERE DEPARTMENTID IS NULL;
-- check the number of rows afterwards
SELECT COUNT(DEPARTMENTID) FROM HumanResources_Department_clean;


