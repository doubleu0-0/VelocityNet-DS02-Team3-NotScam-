-- create analytical reports in sql (Zachariah Loy)
-- this analyses the totaldue for each vendor
SELECT 
    poh."VendorID", 
    v."AccountNumber" AS "VENDOR ACCOUNT", 
    '$' || SUM(poh."TotalDue") AS "TOTAL DUE" 
FROM PURCHASING_PURCHASEORDERHEADER_CLEANED poh 
JOIN PURCHASING_VENDOR_CLEANED v ON poh."VendorID" = v."BusinessEntityID"
GROUP BY poh."VendorID", v."AccountNumber";

--analyse the purchases made for each shipmethod
SELECT 
    sm."Name" AS "SHIPMETHOD", 
    sm."ShipMethodID", 
    COUNT(*) AS "PURCHASES MADE FOR SHIPMETHOD" 
FROM PURCHASING_PURCHASEORDERHEADER_CLEANED poh 
JOIN PURCHASING_SHIPMETHOD_CLEANED sm ON poh."ShipMethodID" = sm."ShipMethodID" 
GROUP BY sm."ShipMethodID", sm."Name"
ORDER BY COUNT(*) DESC;
