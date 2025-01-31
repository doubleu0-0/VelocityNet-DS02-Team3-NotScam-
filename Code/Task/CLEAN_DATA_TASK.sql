-- By Jaden Khoo (S10258662)
-- Create the CLEAN_DATA_TASK to automate cleaning steps, this task must resume first before root tasks
CREATE OR REPLACE TASK CLEAN_DATA_TASK
    WAREHOUSE = TEAM3_WH
    AFTER TRANSFORM_DATA_TASK
    AS
    BEGIN
        --Merge the 2 address columns together
        --Create new column for the full address
        ALTER TABLE PERSON_ADDRESS_CLEANED ADD COLUMN "Address" STRING;
        
        --Add data to the new column and drop the old columns
        UPDATE PERSON_ADDRESS_CLEANED
        SET "Address" = COALESCE("AddressLine1", '') || ' ' || COALESCE("AddressLine2", '');
        ALTER TABLE PERSON_ADDRESS_CLEANED DROP COLUMN "AddressLine1", "AddressLine2";
        
        --Drop unneeded columns
        ALTER TABLE PERSON_ADDRESS_CLEANED DROP COLUMN "rowguid";
        
        --Drop unneeded columns
        ALTER TABLE HUMAN_RESOURCESEMPLOYEE_CLEANED DROP COLUMN "rowguid";
        
        --Drop unneeded columns
        ALTER TABLE SALES_SPECIALOFFERPRODUCT_CLEANED DROP COLUMN "rowguid";
        
        --Drop unneeded columns
        ALTER TABLE SALES_SPECIALOFFER_CLEANED DROP COLUMN "rowguid";

        -- run the cleaning code from the notebook
        EXECUTE NOTEBOOK AUTO_TRANSFORMATION();
    END;

-- resume the task
ALTER TASK CLEAN_DATA_TASK RESUME;
