USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_SCHEMA;
USE SCHEMA TEAM3_SCHEMA;


COPY INTO PURCHASING_PRODUCTVENDOR
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PurchasingProductVendor.csv';

COPY INTO PURCHASING_PURCHASEORDERDETAIL
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PurchasingPurchaseOrderDetail.csv';

COPY INTO PURCHASING_PURCHASEORDERHEADER
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PurchasingPurchaseOrderHeader.csv';

COPY INTO PURCHASING_SHIPMETHOD
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PurchasingShipMethod.csv';

COPY INTO PURCHASING_VENDOR
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PurchasingVendor.csv';

COPY INTO HUMANRESOURCES_DEPARTMENT
FROM @S3_STAGE_TEAM3
PATTERN = 'HumanResourcesDepartment.csv'
FILE_FORMAT = CSVFILEFORMATTEAM3

COPY INTO HUMANRESOURCES_EMPLOYEE
FROM @S3_STAGE_TEAM3
PATTERN = 'HumanResourcesEmployee.csv'
FILE_FORMAT = CSVFILEFORMATTEAM3

COPY INTO SALES_SPECIALOFFER
FROM @S3_STAGE_TEAM3
PATTERN = 'SalesSpecialOffer.csv'
FILE_FORMAT = CSVFILEFORMATTEAM3

COPY INTO SALES_SPECIALOFFERPRODUCT
FROM @S3_STAGE_TEAM3
PATTERN = 'SalesSpecialOfferProduct.csv'
FILE_FORMAT = CSVFILEFORMATTEAM3

COPY INTO Sales_SalesOrderHeader
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'SalesSalesOrderHeader.csv'

COPY INTO Sales_SalesOrderDetail
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'SalesSalesOrderDetail.csv'

COPY INTO Sales_Customer
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'SalesCustomer.csv'

COPY INTO Sales_SalesTerritory
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'SalesSalesTerritory.csv'

COPY INTO Person_StateProvince
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PersonStateProvince.csv'

COPY INTO Person_Address
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'PersonAddress.csv'

COPY INTO Sales_Store
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'SalesStore.csv';

COPY INTO Production_Product
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'ProductionProduct.csv';

COPY INTO Product_Category
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'ProductionProductCategory.csv';

COPY INTO Product_Inventory
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'ProductionProductInventory.csv';

COPY INTO Product_Subcategory
FROM @S3_STAGE_TEAM3
FILE_FORMAT = CSVFILEFORMATTEAM3
PATTERN = 'ProductionProductSubcategory.csv';

