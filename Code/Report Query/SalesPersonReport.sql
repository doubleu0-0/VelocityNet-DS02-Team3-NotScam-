-- By Jaden Khoo (S10258662)
-- sales person report which sales person made the most profit and the contact the most users and their territory group
SELECT 
soh."SalesPersonID",
st."Group",
COUNT(soh."SalesOrderID") AS "Count Of Orders Handled",
SUM("SubTotal") AS "Sub Total",
SUM("TaxAmt") AS "Tax Amt",
SUM("Freight") AS "Freight",
SUM("TotalDue") AS "Total Due"
FROM SALES_SALESORDERHEADER_CLEANED AS soh
LEFT JOIN SALES_SALESTERRITORY_CLEANED AS st
ON soh."TerritoryID" = st."TerritoryID"
WHERE soh."SalesPersonID" IS NOT NULL
GROUP BY st."Group", soh."SalesPersonID"
ORDER BY "Total Due" DESC, "Count Of Orders Handled" DESC;
