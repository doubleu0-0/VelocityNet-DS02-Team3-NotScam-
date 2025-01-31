--zachariah loy yiqi (s10257183D)
--purchase order header fact table (added new date features such as the delivery time for metrics in powerbi visuals)
CREATE OR REPLACE DYNAMIC TABLE FACT_PURCHASE_ORDERHEADER(
    "PurchaseOrderID",
	"RevisionNumber",
	"Status",
	"EmployeeID",
	"VendorID",
	"ShipMethodID",
	"Order_Date" ,
	"Ship_Date" ,
    "OrderYearMonth",
    "MonthOrder",
    "OrderMonth",
    "YearOrder",
    "ShipYearMonth",
    "MonthShip",
    "ShipMonth",
    "DeliveryTime",
    "YearShip",
	"SubTotal" ,
	"TaxAmt",
	"Freight",
	"TotalDue" ,
	"Modified_Date"
) TARGET_LAG = 'DOWNSTREAM' REFRESH_MODE = INCREMENTAL INITIALIZE = ON_CREATE WAREHOUSE = TEAM3_WH
     AS
    SELECT 
    "PurchaseOrderID",
	"RevisionNumber",
	"Status",
	"EmployeeID",
	"VendorID",
	"ShipMethodID",
	"Order_Date" ,
	"Ship_Date" ,
    TO_CHAR("Order_Date", 'YYYY-MM') AS "OrderYearMonth",
    TO_CHAR("Order_Date", 'Mon') AS "MonthOrder",
    EXTRACT(MONTH FROM "Order_Date") AS "OrderMonth",
    TO_CHAR("Order_Date", 'YYYY') AS "YearOrder",
    TO_CHAR("Ship_Date", 'YYYY-MM') AS "ShipYearMonth",
    TO_CHAR("Ship_Date", 'Mon') AS "MonthShip",
    EXTRACT(MONTH FROM "Ship_Date") AS "ShipMonth",
    DATEDIFF(DAY, "Order_Date", "Ship_Date") AS "DeliveryTime",
    TO_CHAR("Order_Date", 'YYYY') AS "YearShip",
	"SubTotal" ,
	"TaxAmt",
	"Freight",
	"TotalDue" ,
	"Modified_Date"
FROM PURCHASING_PURCHASEORDERHEADER_CLEANED;

--ship method dimension table
CREATE OR REPLACE DYNAMIC TABLE DIM_PURCHASING_SHIPMETHOD(
    "ShipMethodID",
    "Name",
    "ShipBase",
    "ShipRate",
    "Modified_Date"
)TARGET_LAG = 'DOWNSTREAM' REFRESH_MODE = INCREMENTAL INITIALIZE = ON_CREATE WAREHOUSE = TEAM3_WH
AS
SELECT 
    "ShipMethodID",
    "Name",
    "ShipBase",
    "ShipRate",
    "Modified_Date"
FROM PURCHASING_SHIPMETHOD_CLEANED;


--fact table purchasing order detail 
CREATE OR REPLACE DYNAMIC TABLE FACT_PURCHASE_ORDERDETAIL (
    "PurchaseOrderID", 
    "PurchaseOrderDetailID", 
    "Due_Date",
    "OrderQty", 
    "ProductID", 
    "UnitPrice", 
    "LineTotal", 
    "ReceivedQty", 
    "RejectedQty", 
    "StockedQty", 
    "Modified_Date" DATE
) TARGET_LAG = 'DOWNSTREAM'
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  WAREHOUSE = TEAM3_WH
AS
SELECT
    "PurchaseOrderID",
    "PurchaseOrderDetailID",
    "Due_Date",
    "OrderQty",
    "ProductID",
    "UnitPrice",
    "LineTotal",
    "ReceivedQty",
    "RejectedQty",
    "StockedQty",
    "Modified_Date"
FROM TEAM3_DB.TEAM3_SCHEMA.PURCHASING_PURCHASEORDERDETAIL_CLEANED;

-- vendor dimension table
CREATE OR REPLACE DYNAMIC TABLE DIM_PURCHASING_VENDOR (
    "BusinessEntityID", 
    "AccountNumber", 
    "Name",
    "CreditRating", 
    "PreferredVendorStatus", 
    "ActiveFlag", 
    "PurchasingWebService_URL", 
    "Modified_Date"
) TARGET_LAG = 'DOWNSTREAM'
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  WAREHOUSE = TEAM3_WH
AS
SELECT
    "BusinessEntityID",
    "AccountNumber",
    "Name",
    "CreditRating",
    "PreferredVendorStatus",
    "ActiveFlag",
    "PurchasingWebService_URL",
    "Modified_Date"
FROM TEAM3_DB.TEAM3_SCHEMA.PURCHASING_VENDOR_CLEANED;


--product vendor  dimension table
CREATE OR REPLACE DYNAMIC TABLE DIM_PURCHASING_PRODUCTVENDOR (
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


--production product fact table (required to access the product inventory table)
CREATE OR REPLACE DYNAMIC TABLE FACT_PRODUCT_PRODUCT (
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

--product inventory dimension table
CREATE OR REPLACE DYNAMIC TABLE DIM_PRODUCTION_PRODUCTINVENTORY (
    "ProductID" ,
    "LocationID" ,
    "Shelf",
    "Bin" ,
    "Quantity",
    "Modified_date"
) TARGET_LAG = 'DOWNSTREAM'
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  WAREHOUSE = TEAM3_WH
AS
SELECT
    "ProductID",
    "LocationID",
    "Shelf",
    "Bin",
    "Quantity",
    "Modified_date"
FROM TEAM3_DB.TEAM3_SCHEMA.PRODUCTION_PRODUCTINVENTORY_CLEANED;
