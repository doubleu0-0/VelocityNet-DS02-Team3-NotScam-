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
