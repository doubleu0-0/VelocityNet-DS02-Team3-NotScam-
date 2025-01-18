import modin.pandas as pd
import pandas
import snowflake.snowpark.modin.plugin
import xmltodict
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, xmlget

pd.set_option("display.max_columns", None)
session = get_active_session()

SalesStoreData = pd.read_snowflake("SALES_STORE")
SalesStoreData = pd.to_pandas(SalesStoreData)
SalesStoreData.head()


# Parse XML data in the column
def parse_xml(xml_data):
    # Parse the XML into a dictionary
    parsed_dict = xmltodict.parse(xml_data)
    # Extract the StoreSurvey section
    store_survey = parsed_dict.get("StoreSurvey", {})
    return store_survey


parsed_columns = SalesStoreData["DEMOGRAPHICS"].apply(parse_xml)

# Create a new DataFrame from the parsed data
parsed_df = pd.json_normalize(parsed_columns)
parsed_df = pd.to_pandas(parsed_df)
parsed_df.head()

# Merge the parsed columns back with the original DataFrame
SalesStoreDataCleaned = pandas.concat([SalesStoreData, parsed_df], axis=1)
SalesStoreDataCleaned.head(5)

# Drop the unnecessary columns
SalesStoreDataCleaned = SalesStoreDataCleaned.drop(columns=["DEMOGRAPHICS", "@xmlns"])
SalesStoreDataCleaned.head()

# Reset index
SalesStoreDataCleaned = SalesStoreDataCleaned.reset_index(drop=True)
# send the cleaned data to the snowfalke db table
SalesStore_df = Session.write_pandas(
    self=session, df=SalesStoreDataCleaned, table_name="SALES_STORE_CLEANED", auto_create_table=True, overwrite=True
)
