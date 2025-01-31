
-- Btw for some of them I had to recreate the tags as I gave the wrong permissions


-- For first name
CREATE OR REPLACE MASKING POLICY team3_name_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        ELSE CONCAT(UPPER(SUBSTRING(val, 1, 1)), REPEAT('*', LENGTH(val) - 1))
    END;

ALTER TAG FIRST_NAME_TAG
    SET MASKING POLICY team3_name_final_mask;

    
-- For middle name
CREATE OR REPLACE MASKING POLICY team3_middle_name_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        WHEN LENGTH(val) = 1 THEN val
        ELSE CONCAT(SUBSTRING(val, 1, 1), REPEAT('*', LENGTH(val) - 1))
    END;

ALTER TAG MIDDLE_NAME_TAG
    SET MASKING POLICY team3_middle_name_final_mask;

    
-- For last name
CREATE OR REPLACE MASKING POLICY team3_last_name_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        ELSE CONCAT(SUBSTRING(val, 1, 1), REPEAT('*', LENGTH(val) - 1))
    END;

ALTER TAG LAST_NAME_TAG
    SET MASKING POLICY team3_last_name_final_mask;

    
-- For AccountNumber
CREATE OR REPLACE MASKING POLICY team3_account_number_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        ELSE '**********'
    END;

ALTER TAG ACCOUNT_NUMBER_TAG_FINAL
    SET MASKING POLICY team3_account_number_final_mask;

    
-- For PostalCode
CREATE OR REPLACE MASKING POLICY team3_postal_code_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        ELSE CONCAT(SUBSTRING(val, 1, 2), REPEAT('*', LENGTH(val) - 2))
    END;

ALTER TAG POSTAL_CODE_FINAL_TAG
    SET MASKING POLICY team3_postal_code_final_mask;

    
-- For Address
CREATE OR REPLACE MASKING POLICY team3_address_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        ELSE '**********'
    END;

ALTER TAG ADDRESS_TAG_FINAL
    SET MASKING POLICY team3_address_final_mask;

    
-- LoginId 
CREATE OR REPLACE MASKING POLICY team3_login_id_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        WHEN val IS NULL THEN NULL
        ELSE REGEXP_REPLACE(val, '\\\\(\\w+)$', '\\\\*****')
    END;

ALTER TAG LOGIN_ID_TAG_FINAL
    SET MASKING POLICY team3_login_id_final_mask;

    
-- NationalID
CREATE OR REPLACE MASKING POLICY team3_national_id_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        ELSE CONCAT(REPEAT('*', LENGTH(val) - 4), SUBSTRING(val, -4))
    END;

ALTER TAG NATIONAL_ID_NUMBER_TAG_FINAL
    SET MASKING POLICY team3_national_id_final_mask;

    
-- CreditCardApprovalCode
CREATE OR REPLACE MASKING POLICY team3_credit_card_code_final_mask AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        ELSE '************'
    END;

ALTER TAG CREDIT_CARD_APPROVAL_TAG
    SET MASKING POLICY team3_credit_card_code_final_mask;


    
-- CreditCardNumber
CREATE OR REPLACE MASKING POLICY team3_card_number_final_mask4 AS (val INT)
RETURNS INT ->
    CASE
        WHEN CURRENT_ROLE() IN ('TEAM3_MASTER_ADMIN') THEN val
        ELSE 00000000000
    END;

ALTER TAG CARD_NUMBER_TAG_FINAL4
    SET MASKING POLICY team3_card_number_final_mask4;


CREATE OR REPLACE TAG TEAM3_SCHEMA.CARD_NUMBER_TAG_FINAL4
COMMENT = 'CardNumber';



ALTER TABLE TEAM3_SCHEMA.SALES_CREDITCARD_CLEANED
MODIFY COLUMN "CardNumber"
SET TAG TEAM3_SCHEMA.CARD_NUMBER_TAG_FINAL4 = 'CardNumber';
