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
FROM PRODUCTION_PRODUCTINVENTORY_CLEANED AS i
INNER JOIN PRODUCTION_PRODUCT_CLEANED AS p ON i."ProductID" = p."ProductID"
WHERE i."Quantity"> 0
ORDER BY TotalInventoryValue DESC;
