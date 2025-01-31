-- By Tey Xue Cong (S10257059H)
USE ROLE TEAM3_MASTER_ADMIN;
CREATE WAREHOUSE IF NOT EXISTS TEAM3_WH;
USE WAREHOUSE TEAM3_WH;
CREATE DATABASE IF NOT EXISTS TEAM3_DB;
USE TEAM3_DB.PUBLIC;
CREATE SCHEMA IF NOT EXISTS TEAM3_SCHEMA;
USE SCHEMA TEAM3_SCHEMA;

-- Analyze stock levels and calculate total inventory value, and flag if the stock is low
SELECT 
    p."Name" AS Product,
    i."LocationID",
    i."Quantity" AS StockLevel,
    (i."Quantity" * p."StandardCost") AS TotalInventoryValue,
    p."SafetyStockLevel",
    p."ReorderPoint",
    CASE 
        WHEN i."Quantity" < p."ReorderPoint" THEN 'Reorder Needed'
        WHEN i."Quantity" >= p."SafetyStockLevel" THEN 'Stock is Safe'
        ELSE 'Stock Level Warning'
    END AS StockStatus
FROM PRODUCTION_PRODUCTINVENTORY_CLEANED i
JOIN PRODUCTION_PRODUCT_CLEANED p ON i."ProductID" = p."ProductID"
WHERE i."Quantity"> 0
ORDER BY TotalInventoryValue DESC;

