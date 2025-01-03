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
    SalariedFlag NUMBER(1,0) NOT NULL DEFAULT 1,
    VacationHours NUMBER(38, 0) NOT NULL DEFAULT 0,
    SickLeaveHours NUMBER(38, 0) NOT NULL DEFAULT 0,
    CurrentFlag NUMBER(1,0) NOT NULL DEFAULT 1,
    rowguid STRING NOT NULL DEFAULT UUID_STRING(),
    ModifiedDate STRING
);
