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
