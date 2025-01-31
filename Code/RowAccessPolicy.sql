--create a role policy to map the roles to their territiryID
CREATE OR REPLACE TABLE TEAM3_DB.TEAM3_SCHEMA.ROLE_POLICY_MAP
(
ROLE STRING,
TERRITORYID NUMBER
);

--append the values in to the row policy map table
insert into TEAM3_DB.TEAM3_SCHEMA.ROLE_POLICY_MAP (ROLE, TERRITORYID)
    select
    case
        when "Group" = 'North America' THEN 'TEAM3_ANALYST_NORTHAMERICA'
        when "Group" = 'Europe' THEN 'TEAM3_ANALYST_EUROPE'
        when "Group" = 'Pacific' THEN 'TEAM3_ANALYST_PACIFIC'
    end AS role,
    "TerritoryID"
from sales_salesterritory_cleaned;

SELECT * FROM ROLE_POLICY_MAP


-- create a row access policy object using the previously made table, exclude administrative roles that will be affected by this policy
create or replace row access policy rap_dim_territory_policy
as (TERRITORYID NUMBER) RETURNS BOOLEAN ->
CURRENT_ROLE() in
/* list of roles that will not be subject to the policy */
(
'TEAM3_MASTER_ADMIN','TEAM3_ANALYST_GLOBAL', 'TEAM3_DEVELOPER', 'TEAM3_TESTER'
)
or exists
/* this clause references our mapping table from above to handle
the row level filtering */
(
select rp.ROLE
from ROLE_POLICY_MAP rp
where 1=1
and rp.ROLE = CURRENT_ROLE()
and rp.TERRITORYID = TerritoryID
);

--choose tables to attach the policy to
-- SELECT * FROM TEAM3_SCHEMA.PERSON_ADDRESS_CLEANED; --YES
-- SELECT * FROM TEAM3_SCHEMA.PERSON_STATEPROVINCE_CLEANED; -- YES
-- SELECT * FROM TEAM3_SCHEMA.SALES_CUSTOMER_CLEANED; --YES
-- SELECT * FROM TEAM3_SCHEMA.SALES_SALESPERSON_CLEANED; --YES
-- SELECT * FROM TEAM3_SCHEMA.SALES_SALESTERRITORY_CLEANED; --YES


--Lastly, apply the row policy to our fact tables
alter table person_stateprovince_cleaned
add row access policy rap_dim_territory_policy ON
("TerritoryID");

alter table sales_customer_cleaned
add row access policy rap_dim_territory_policy ON
("TerritoryID");

alter table sales_salesperson_cleaned
add row access policy rap_dim_territory_policy ON
("TerritoryID");

alter table sales_salesterritory_cleaned
add row access policy rap_dim_territory_policy ON
("TerritoryID");

--drop in case the row access policy has issues
alter table person_stateprovince_cleaned
drop row access policy rap_dim_territory_policy;

alter table sales_salesperson_cleaned
drop row access policy rap_dim_territory_policy;

alter table sales_salesterritory_cleaned
drop row access policy rap_dim_territory_policy;

alter table sales_customer_cleaned
drop row access policy rap_dim_territory_policy;

--testing
use role team3_analyst_europe;
select * from sales_salesterritory_cleaned;
use role team3_analyst_pacific;
select * from sales_salesperson_cleaned;
use role team3_analyst_northamerica;
select * from sales_salesperson_cleaned;

use role team3_analyst_europe;
select * from person_stateprovince_cleaned;
use role team3_analyst_pacific;
select * from person_stateprovince_cleaned;
use role team3_analyst_northamerica;
select * from person_stateprovince_cleaned;

use role team3_analyst_europe;
select * from sales_customer_cleaned;
use role team3_analyst_pacific;
select * from sales_customer_cleaned;
use role team3_analyst_northamerica;
select * from sales_customer_cleaned;

use role team3_analyst_europe;
select * from sales_salesperson_cleaned;
use role team3_analyst_pacific;
select * from sales_salesperson_cleaned;
use role team3_analyst_northamerica;
select * from sales_salesperson_cleaned;

--switch to admin last
use role team3_master_admin;
