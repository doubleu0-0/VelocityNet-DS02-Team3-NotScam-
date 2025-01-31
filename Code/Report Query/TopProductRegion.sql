-- By Tey Xue Cong (S10257059H)
-- View to display the top 5 products by sales revenue for each territory
CREATE OR REPLACE VIEW VIEW_TOP_PRODUCTS_BY_REGION AS
WITH RankedProducts AS (
    SELECT 
        SST."Name" AS TerritoryName,
        PP."Name" AS ProductName,
        SUM(SSD."LineTotal") AS TotalSales,
        RANK() OVER (PARTITION BY SST."Name"
ORDER BY SUM(SSD."LineTotal") DESC NULLS LAST) AS RankInTerritory
    FROM SALES_SALESTERRITORY_CLEANED AS SST
    INNER JOIN SALES_SALESORDERHEADER_CLEANED AS SSH
    ON SST."TerritoryID" = SSH."TerritoryID"
    INNER JOIN SALES_SALESORDERDETAIL_CLEANED AS SSD
    ON SSH."SalesOrderID" = SSD."SalesOrderID"
    INNER JOIN PRODUCTION_PRODUCT_CLEANED AS PP
    ON SSD."ProductID" = PP."ProductID"
    GROUP BY SST."Name", PP."Name"
)
SELECT *
FROM RankedProducts
WHERE RankInTerritory <= 5
ORDER BY TerritoryName, RankInTerritory;

SELECT * FROM VIEW_TOP_PRODUCTS_BY_REGION;
