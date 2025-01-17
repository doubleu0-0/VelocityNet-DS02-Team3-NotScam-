CREATE OR REPLACE TABLE Sales_Store (
    BusinessEntityID INT NOT NULL PRIMARY KEY,
    Name VARCHAR(50),
    SalesPersonID INT,
    Demographics STRING,
    rowguid VARCHAR(36),
    ModifiedDate STRING
);


CREATE OR REPLACE TABLE Production_Product (
    ProductID INT AUTOINCREMENT NOT NULL,
    Name STRING NOT NULL,
    ProductNumber STRING(25) NOT NULL,
    MakeFlag BOOLEAN NOT NULL DEFAULT TRUE,
    FinishedGoodsFlag BOOLEAN NOT NULL DEFAULT TRUE,
    Color STRING(15),
    SafetyStockLevel SMALLINT NOT NULL,
    ReorderPoint SMALLINT NOT NULL,
    StandardCost NUMBER NOT NULL,
    ListPrice NUMBER NOT NULL,
    Size STRING(5),
    SizeUnitMeasureCode CHAR(4),
    WeightUnitMeasureCode CHAR(4),
    Weight DECIMAL,
    DaysToManufacture INT NOT NULL,
    ProductLine CHAR(2),
    Class CHAR(2),
    Style CHAR(2),
    ProductSubcategoryID INT,
    ProductModelID INT,
    SellStartDate TIMESTAMP NOT NULL,
    SellEndDate TIMESTAMP,
    DiscontinuedDate TIMESTAMP,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING
);


CREATE OR REPLACE TABLE Product_Category (
    ProductCategoryID INT AUTOINCREMENT NOT NULL PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    rowguid VARCHAR(36) NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING NOT NULL
);


CREATE OR REPLACE TABLE Product_Inventory (
    ProductID INT NOT NULL,
    LocationID SMALLINT NOT NULL,
    Shelf STRING(10) NOT NULL,
    Bin TINYINT NOT NULL,
    Quantity SMALLINT NOT NULL DEFAULT 0,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING NOT NULL
);

CREATE OR REPLACE TABLE Product_Subcategory (
    ProductSubcategoryID INT AUTOINCREMENT NOT NULL,
    ProductCategoryID INT NOT NULL,
    Name STRING NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING NOT NULL -- Default timestamp
);

SELECT * FROM PRODUCT_SUBCATEGORY;
CREATE OR REPLACE TABLE Sales_Store_Clean (
    BusinessEntityID INT NOT NULL PRIMARY KEY,
    Name VARCHAR(50),
    SalesPersonID INT,
    Demographics STRING,
    rowguid VARCHAR(36),
    ModifiedDate STRING,
    AnnualSales NUMBER,
    AnnualRevenue NUMBER,
    BankName STRING,
    BusinessType STRING,
    YearOpened INT,
    Specialty STRING,
    SquareFeet INT,
    Brands STRING,
    Internet STRING,
    NumberEmployees INT
);



CREATE OR REPLACE TABLE SALES_SPECIALOFFERPRODUCT (
    SPECIALOFFERID NUMBER(38, 0) NOT NULL,
    PRODUCTID NUMBER(38, 0) NOT NULL,
    ROWGUID STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING
);

CREATE OR REPLACE TABLE SALES_SPECIALOFFER (
    SpecialOfferID NUMBER(38, 0) NOT NULL AUTOINCREMENT,
    Description STRING NOT NULL,
    DiscountPct NUMBER(5, 2) NOT NULL DEFAULT 0.00,
    Type STRING NOT NULL,
    Category STRING NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ NOT NULL,
    MinQty NUMBER(38, 0) NOT NULL DEFAULT 0,
    MaxQty NUMBER(38, 0) NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING
);
CREATE OR REPLACE TABLE HUMANRESOURCES_DEPARTMENT (
    DepartmentID NUMBER(38, 0) AUTOINCREMENT PRIMARY KEY,   -- Auto-increment for DepartmentID
    Name STRING NOT NULL,                                   -- String for Name
    GroupName STRING NOT NULL,                              -- String for GroupName
    ModifiedDate STRING
);

CREATE OR REPLACE TABLE HUMANRESOURCES_EMPLOYEE (
    BusinessEntityID NUMBER(38, 0) NOT NULL,
    NationalIDNumber STRING(15) NOT NULL,
    LoginID STRING(256) NOT NULL,
    OrganizationNode STRING,
    OrganizationLevel STRING,
    JobTitle STRING(50) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus STRING(1) NOT NULL,
    Gender STRING(1) NOT NULL,
    HireDate DATE NOT NULL,
    SalariedFlag NUMBER(1, 0) NOT NULL DEFAULT 1,
    VacationHours NUMBER(38, 0) NOT NULL DEFAULT 0,
    SickLeaveHours NUMBER(38, 0) NOT NULL DEFAULT 0,
    CurrentFlag NUMBER(1, 0) NOT NULL DEFAULT 1,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING
);

CREATE OR REPLACE TABLE Purchasing_PurchaseOrderDetail (
    PurchaseOrderID INT NOT NULL, -- Foreign key reference (no enforcement in Snowflake)
    PurchaseOrderDetailID INT AUTOINCREMENT NOT NULL, -- Replaces SQL Server's IDENTITY
    DueDate DATETIME NOT NULL,
    OrderQty SMALLINT NOT NULL,
    ProductID INT NOT NULL,
    UnitPrice DECIMAL(19, 4) NOT NULL, -- Replaces MONEY
    LineTotal DECIMAL(19, 4) NOT NULL, -- Computed column
    ReceivedQty DECIMAL(8, 2) NOT NULL,
    RejectedQty DECIMAL(8, 2) NOT NULL,
    StockedQty DECIMAL(8, 2) NOT NULL, -- Computed column
    ModifiedDate STRING
);

CREATE OR REPLACE TABLE Purchasing_ProductVendor (
    ProductID INT NOT NULL,
    BusinessEntityID INT NOT NULL,
    AverageLeadTime INT NOT NULL,
    StandardPrice DECIMAL(19, 4) NOT NULL, -- Replaces MONEY
    LastReceiptCost DECIMAL(19, 4), -- Replaces MONEY
    LastReceiptDate DATETIME, -- NULL is implicit
    MinOrderQty INT NOT NULL,
    MaxOrderQty INT NOT NULL,
    OnOrderQty INT, -- NULL is implicit
    UnitMeasureCode CHAR(3) NOT NULL, -- Replaces NCHAR(3)
    ModifiedDate STRING
);


CREATE OR REPLACE TABLE Purchasing_PurchaseOrderHeader (
    PurchaseOrderID INT AUTOINCREMENT NOT NULL, -- Replaces IDENTITY (1, 1)
    RevisionNumber TINYINT NOT NULL DEFAULT 0,
    Status TINYINT NOT NULL DEFAULT 1,
    EmployeeID INT NOT NULL,
    VendorID INT NOT NULL,
    ShipMethodID INT NOT NULL,
    OrderDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Replaces DEFAULT (GETDATE())
    ShipDate DATETIME, -- NULL is implicit
    SubTotal DECIMAL(19, 4) NOT NULL DEFAULT 0.00, -- Replaces MONEY
    TaxAmt DECIMAL(19, 4) NOT NULL DEFAULT 0.00, -- Replaces MONEY
    Freight DECIMAL(19, 4) NOT NULL DEFAULT 0.00, -- Replaces MONEY
    TotalDue DECIMAL(19, 4) NOT NULL, -- Computed column
    ModifiedDate STRING
);

CREATE OR REPLACE TABLE Purchasing_Vendor (
    BusinessEntityID INT NOT NULL,
    AccountNumber STRING NOT NULL, -- Replace with STRING for variable-length text
    Name STRING NOT NULL, -- Replace with STRING for variable-length text
    CreditRating TINYINT NOT NULL,
    PreferredVendorStatus BOOLEAN NOT NULL DEFAULT TRUE, -- Replaces Flag (assuming TRUE/FALSE values)
    ActiveFlag BOOLEAN NOT NULL DEFAULT TRUE, -- Replaces Flag (assuming TRUE/FALSE values)
    PurchasingWebServiceURL STRING, -- Replaces NVARCHAR (1024)
    ModifiedDate STRING
);

CREATE OR REPLACE TABLE Purchasing_ShipMethod (
    ShipMethodID INT AUTOINCREMENT NOT NULL, -- Replaces IDENTITY (1, 1)
    Name STRING NOT NULL, -- Replace with STRING for variable-length text
    ShipBase DECIMAL(19, 4) NOT NULL DEFAULT 0.00, -- Replaces MONEY
    ShipRate DECIMAL(19, 4) NOT NULL DEFAULT 0.00, -- Replaces MONEY
    rowguid STRING NOT NULL DEFAULT UUID_STRING(), -- Replaces uniqueidentifier and NEWID() with UUID_STRING()
    ModifiedDate STRING
);

-- sales tables
CREATE OR ALTER TABLE TEAM3_SCHEMA.Sales_SalesOrderHeader (
    SalesOrderID INT,
    RevisionNumber INT,
    OrderDate TIMESTAMP,
    DueDate TIMESTAMP,
    ShipDate TIMESTAMP,
    Status INT,
    OnlineOrderFlag INT,
    SalesOrderNumber STRING,
    PurchaseOrderNumber STRING,
    AccountNumber STRING,
    CustomerID INT,
    SalesPersonID INT,
    TerritoryID INT,
    BillToAddressID INT,
    ShipToAddressID INT,
    ShipMethodID INT,
    CreditCardID INT,
    CreditCardApprovalCode STRING,
    CurrencyRateID INT NULL,
    SubTotal DECIMAL,
    TaxAmt DECIMAL,
    Freight DECIMAL,
    TotalDue DECIMAL,
    Comment STRING NULL,
    ROWGUID STRING,
    MODIFIEDDATE TIMESTAMP
);

CREATE OR ALTER TABLE TEAM3_SCHEMA.Sales_SalesOrderDetail (
    SalesOrderID INT,
    SalesOrderDetailID INT,
    CarrierTrackingNumber STRING,
    OrderQty INT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice DECIMAL,
    UnitPriceDiscount DECIMAL,
    LineTotal DECIMAL,
    ROWGUID STRING,
    MODIFIEDDATE TIMESTAMP
);

CREATE OR ALTER TABLE TEAM3_SCHEMA.Sales_Customer (
    CustomerID INT,
    PersonID INT NULL,
    StoreID INT,
    TerritoryID INT,
    AccountNumber STRING,
    ROWGUID STRING,
    MODIFIEDDATE TIMESTAMP
);

CREATE OR ALTER TABLE TEAM3_SCHEMA.Sales_SalesTerritory (
    TerritoryID INT,
    Name STRING,
    CountryRegionCode STRING,
    "Group" STRING,
    SalesYTD DECIMAL,
    SalesLastYear DECIMAL,
    CostYTD DECIMAL,
    CostLastYear DECIMAL,
    ROWGUID STRING,
    MODIFIEDDATE TIMESTAMP
);

-- Person table
CREATE OR ALTER TABLE TEAM3_SCHEMA.Person_StateProvince (
    StateProvinceID INT,
    StateProvinceCode STRING,
    CountryRegionCode STRING,
    IsOnlyStateProvinceFlag INT,
    Name STRING,
    TerritoryID INT,
    ROWGUID STRING,
    MODIFIEDDATE TIMESTAMP
);


CREATE OR ALTER TABLE TEAM3_SCHEMA.Person_Address (
    AddressID INT,
    AddressLine1 STRING,
    AddressLine2 STRING NULL,
    City STRING,
    StateProvinceID INT,
    PostalCode STRING,
    SpatialLocation STRING,
    ROWGUID STRING,
    MODIFIEDDATE TIMESTAMP
);

-- HUMANRESOURCES
CREATE OR REPLACE TABLE HUMANRESOURCES_DEPARTMENT (
    DepartmentID NUMBER(38, 0) AUTOINCREMENT PRIMARY KEY, 
    Name STRING NOT NULL, 
    GroupName STRING NOT NULL, 
    ModifiedDate TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP  
);

CREATE OR REPLACE TABLE HUMANRESOURCES_EMPLOYEE (
    BusinessEntityID NUMBER(38, 0) NOT NULL,
    NationalIDNumber STRING(15) NOT NULL,
    LoginID STRING(256) NOT NULL,
    OrganizationNode STRING,
    OrganizationLevel STRING,
    JobTitle STRING(50) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus STRING(1) NOT NULL,
    Gender STRING(1) NOT NULL,
    HireDate DATE NOT NULL,
    SalariedFlag NUMBER(1, 0) NOT NULL DEFAULT 1, 
    VacationHours NUMBER(38, 0) NOT NULL DEFAULT 0, 
    SickLeaveHours NUMBER(38, 0) NOT NULL DEFAULT 0, 
    CurrentFlag NUMBER(1, 0) NOT NULL DEFAULT 1, 
    rowguid STRING NOT NULL DEFAULT UUID_STRING(), 
    ModifiedDate TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
-- SALES
CREATE OR REPLACE TABLE SALES_SPECIALOFFERPRODUCT (
    SpecialOfferID NUMBER(38, 0) NOT NULL,
    ProductID NUMBER(38, 0) NOT NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SALES_SPECIALOFFER (
    SpecialOfferID NUMBER(38, 0) NOT NULL AUTOINCREMENT,
    Description STRING NOT NULL,
    DiscountPct NUMBER(5, 2) NOT NULL DEFAULT 0.00,
    Type STRING NOT NULL,
    Category STRING NOT NULL,
    StartDate TIMESTAMP_NTZ NOT NULL,
    EndDate TIMESTAMP_NTZ NOT NULL,
    MinQty NUMBER(38, 0) NOT NULL DEFAULT 0,
    MaxQty NUMBER(38, 0) NULL,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);




