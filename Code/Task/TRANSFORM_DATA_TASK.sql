-- By Jaden Khoo (S10258662)
-- create task to transform data which will run after the TEAM3_TABLE_TRIGGERED_TASK gets the data from AWS
CREATE OR REPLACE TASK TRANSFORM_DATA_TASK
    WAREHOUSE = TEAM3_WH    
    AFTER CREATE_STREAMS_TASK
    WHEN
    -- check if there are any changes
    SYSTEM$STREAM_HAS_DATA('HUMAN_RESOURCESDEPARTMENT_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('HUMAN_RESOURCESEMPLOYEE_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PERSON_ADDRESS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PERSON_PERSON_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PERSON_STATEPROVINCE_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PRODUCTION_PRODUCT_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PRODUCTION_PRODUCTCATEGORY_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PRODUCTION_PRODUCTINVENTORY_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PRODUCTION_PRODUCTSUBCATEGORY_STREAM')
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
    OR SYSTEM$STREAM_HAS_DATA('PURCHASING_SHIPMETHOD_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('PURCHASING_VENDOR_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('SALES_SPECIALOFFER_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('SALES_CUSTOMER_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('SALES_STORE_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('SALES_SPECIALOFFERPRODUCT_STREAM')
    AS
    BEGIN
        -- cleaning for Sales_SalesTerritory
        CREATE OR REPLACE TABLE SALES_SALESTERRITORY_CLEANED AS
        SELECT 
        "TerritoryID",
        "Name", 
        "CountryRegionCode",
        "Group",
        "SalesYTD",
        "SalesLastYear",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SALESTERRITORY;
        
        -- cleaning for Sales_Customer
        CREATE OR REPLACE TABLE SALES_CUSTOMER_CLEANED AS
        SELECT 
        "CustomerID",
        "PersonID",
        "StoreID",
        "TerritoryID",
        "AccountNumber",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_CUSTOMER;
        
        -- cleaning for Sales_SalesOrderHeader
        CREATE OR REPLACE TABLE SALES_SALESORDERHEADER_CLEANED AS
        SELECT 
        "SalesOrderID",
        "OrderDate"::DATE AS "OrderDate",
        "DueDate"::DATE AS "DueDate",
        "ShipDate"::DATE AS "ShipDate",
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
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SALESORDERHEADER;
        
        -- cleaning for Sales_SalesOrderDetail
        CREATE OR REPLACE TABLE SALES_SALESORDERDETAIL_CLEANED AS
        SELECT 
        "SalesOrderID",
        "SalesOrderDetailID",
        "CarrierTrackingNumber",
        "OrderQty",
        "ProductID",
        "SpecialOfferID",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SALESORDERDETAIL;
        
        -- cleaning for HumanResources_Department
        CREATE OR REPLACE TABLE HUMAN_RESOURCESDEPARTMENT_CLEANED AS
        SELECT 
        "DepartmentID",
        "Name",
        "GroupName",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM HUMAN_RESOURCESDEPARTMENT;
        
        -- cleaning_ for PURCHASING_PRODUCTVENDOR
        CREATE OR REPLACE TABLE PURCHASING_PRODUCTVENDOR_CLEANED AS 
        SELECT 
        "ProductID",
        "BusinessEntityID",
        "AverageLeadTime",
        "StandardPrice",
        "LastReceiptCost",
        "LastReceiptDate"::DATE AS "LastReceipt_Date",
        "MinOrderQty",
        "MaxOrderQty",
        COALESCE("OnOrderQty", 0) AS "OnOrder_Qty",
        "UnitMeasureCode",
        "ModifiedDate"::DATE AS "Modified_Date"
        FROM PURCHASING_PRODUCTVENDOR
        WHERE "Modified_Date" IS NOT NULL AND "LastReceipt_Date" IS NOT NULL;
        
        -- cleaning for PURCHASING_PURCHASEORDERDETAIL
        CREATE OR REPLACE TABLE PURCHASING_PURCHASEORDERDETAIL_CLEANED AS 
        SELECT
        "PurchaseOrderID",
        "PurchaseOrderDetailID",
        "DueDate"::DATE AS "Due_Date",
        "OrderQty",
        "ProductID",
        "UnitPrice",
        "LineTotal",
        "ReceivedQty",
        "RejectedQty", 
        "StockedQty",
        "ModifiedDate"::DATE AS "Modified_Date"
        FROM PURCHASING_PURCHASEORDERDETAIL
        WHERE "Modified_Date" IS NOT NULL AND "Due_Date" IS NOT NULL;
        
        -- cleaning for PURCHASING_SHIPMETHOD
        CREATE OR REPLACE TABLE PURCHASING_SHIPMETHOD_CLEANED AS
        SELECT
        "ShipMethodID",
        "Name",
        "ShipBase",
        "ShipRate",
        "ModifiedDate"::DATE AS "Modified_Date"
        FROM PURCHASING_SHIPMETHOD
        WHERE "Modified_Date" IS NOT NULL;
        
        -- cleaning for PURCHASING_PURCHASEORDERHEADER
        CREATE OR REPLACE TABLE PURCHASING_PURCHASEORDERHEADER_CLEANED AS 
        SELECT
        "PurchaseOrderID",
        "RevisionNumber",
        "Status",
        "EmployeeID",
        "VendorID",
        "ShipMethodID",
        "OrderDate"::DATE AS "Order_Date",
        "ShipDate"::DATE AS "Ship_Date",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
        "ModifiedDate"::DATE AS "Modified_Date"
        FROM PURCHASING_PURCHASEORDERHEADER
        WHERE "Ship_Date" IS NOT NULL AND "Order_Date" IS NOT NULL AND "Modified_Date" IS NOT NULL;
        
        -- cleaning for PURCHASING_VENDOR
        CREATE OR REPLACE TABLE PURCHASING_VENDOR_CLEANED AS 
        SELECT
        "BusinessEntityID",
        "AccountNumber",
        "Name",
        "CreditRating",
        "PreferredVendorStatus",
        "ActiveFlag",
        COALESCE("PurchasingWebServiceURL", 'no_url') AS "PurchasingWebService_URL",
        "ModifiedDate"::DATE AS "Modified_Date"
        FROM PURCHASING_VENDOR
        WHERE "Modified_Date" IS NOT NULL;
        
        -- Cleaning Product_Subcategory
        CREATE OR REPLACE TABLE PRODUCTION_PRODUCTSUBCATEGORY_CLEANED AS 
        SELECT
        "ProductSubcategoryID",
        "ProductCategoryID",
        "Name"
        FROM PRODUCTION_PRODUCTSUBCATEGORY
        WHERE "Name" IS NOT NULL;
        
        -- Cleaning Product_Inventory
        CREATE OR REPLACE TABLE PRODUCTION_PRODUCTINVENTORY_CLEANED AS
        SELECT
        "ProductID",
        "LocationID",
        "Shelf",
        "Bin",
        "Quantity",
        "ModifiedDate"::DATE AS "Modified_date"
        FROM PRODUCTION_PRODUCTINVENTORY
        WHERE "ModifiedDate" IS NOT NULL AND "ProductID" IS NOT NULL;
        
        -- Cleaning Product_Category
        CREATE OR REPLACE TABLE PRODUCTION_PRODUCTCATEGORY_CLEANED AS
        SELECT
        "ProductCategoryID",
        "Name"
        FROM PRODUCTION_PRODUCTCATEGORY
        WHERE "Name" IS NOT NULL;
        
        -- Cleaning Sales_Store table
        CREATE OR REPLACE TABLE SALES_STORE_CLEANED AS
        SELECT
        "BusinessEntityID",
        "Name",
        "SalesPersonID",
        "Demographics"
        FROM SALES_STORE
        WHERE "Name" IS NOT NULL;
        
        -- Clean Production_Product
        CREATE OR REPLACE TABLE PRODUCTION_PRODUCT_CLEANED AS
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
        "SellStartDate"::DATE AS "SellStartDate",
        "SellEndDate"::DATE AS "SellEndDate",
        "DiscontinuedDate"::DATE AS "DiscontinuedDate"
        FROM PRODUCTION_PRODUCT
        WHERE "Name" IS NOT NULL AND "ProductNumber" IS NOT NULL AND "SellStartDate" IS NOT NULL AND "ListPrice" != 0 AND "ProductSubcategoryID" IS NOT NULL AND "ProductModelID" IS NOT NULL;
        
        -- Clean PERSON_STATEPROVINCE
        CREATE OR REPLACE TABLE PERSON_STATEPROVINCE_CLEANED AS
        SELECT 
        "StateProvinceID",
        "StateProvinceCode",
        "CountryRegionCode",
        "IsOnlyStateProvinceFlag",
        "Name",
        "TerritoryID",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM PERSON_STATEPROVINCE;
        
        -- Clean SALES_SPECIALOFFERPRODUCT
        CREATE OR REPLACE TABLE SALES_SPECIALOFFERPRODUCT_CLEANED AS
        SELECT 
        "SpecialOfferID",
        "ProductID",
        "rowguid",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SPECIALOFFERPRODUCT;
        
        -- Clean SALES_SPECIALOFFER
        CREATE OR REPLACE TABLE SALES_SPECIALOFFER_CLEANED AS
        SELECT 
        "SpecialOfferID",
        "Description",
        "DiscountPct",
        "Type",
        "Category",
        "StartDate",
        "EndDate",
        "MinQty",
        "MaxQty",
        "rowguid",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SPECIALOFFER;
        
        -- Clean PERSON_ADDRESS
        CREATE OR REPLACE TABLE PERSON_ADDRESS_CLEANED AS
        SELECT 
        "AddressID",
        "AddressLine1",
        "AddressLine2",
        "City",
        "StateProvinceID",
        "PostalCode",
        "SpatialLocation",
        "rowguid",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM PERSON_ADDRESS;
        
        -- Clean HUMANRESOURCES_EMPLOYEE
        CREATE OR REPLACE TABLE HUMAN_RESOURCESEMPLOYEE_CLEANED AS
        SELECT 
        "BusinessEntityID",
        "NationalIDNumber",
        "LoginID",
        "OrganizationNode",
        "OrganizationLevel",
        "JobTitle",
        "BirthDate",
        "MaritalStatus",
        "Gender",
        "HireDate",
        "SalariedFlag",
        "VacationHours",
        "SickLeaveHours",
        "CurrentFlag",
        "rowguid",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM HUMAN_RESOURCESEMPLOYEE;
        
        -- clean PERSON_PERSON
        CREATE OR REPLACE TABLE PERSON_PERSON_CLEANED AS
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
        "AdditionalContactInfo",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM PERSON_PERSON;
        
        -- clean SALES_SALESREASON
        CREATE OR REPLACE TABLE SALES_SALESREASON_CLEANED AS
        SELECT 
        "SalesReasonID",
        "Name",
        "ReasonType",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SALESREASON;
        
        -- clean SALES_CREDITCARD
        CREATE OR REPLACE TABLE SALES_CREDITCARD_CLEANED AS
        SELECT 
        "CreditCardID",
        "CardType",
        "CardNumber",
        "ExpMonth",
        "ExpYear",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_CREDITCARD;
        
        -- clean SALES_SALESPERSONQUOTAHISTORY
        CREATE OR REPLACE TABLE SALES_SALESPERSONQUOTAHISTORY_CLEANED AS
        SELECT 
        "BusinessEntityID",
        "QuotaDate",
        "SalesQuota",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SALESPERSONQUOTAHISTORY;
        
        -- clean SALES_CURRENCY
        CREATE OR REPLACE TABLE SALES_CURRENCY_CLEANED AS
        SELECT 
        "CurrencyCode",
        "Name",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_CURRENCY;
        
        -- clean SALES_CURRENCYRATE
        CREATE OR REPLACE TABLE SALES_CURRENCYRATE_CLEANED AS
        SELECT 
        "CurrencyRateID",
        "CurrencyRateDate",
        "FromCurrencyCode",
        "ToCurrencyCode",
        "AverageRate",
        "EndOfDayRate",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_CURRENCYRATE;
        
        -- clean SALES_SALESPERSON
        CREATE OR REPLACE TABLE SALES_SALESPERSON_CLEANED AS
        SELECT 
        "BusinessEntityID",
        "TerritoryID",
        "SalesQuota",
        "Bonus",
        "CommissionPct",
        "SalesYTD",
        "SalesLastYear",
        "ModifiedDate"::DATE AS "ModifiedDate"
        FROM SALES_SALESPERSON;
    END;

-- resume the task
ALTER TASK TRANSFORM_DATA_TASK RESUME;
