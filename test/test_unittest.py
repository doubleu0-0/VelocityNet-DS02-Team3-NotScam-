import modin.pandas as pd
from snowflake.snowpark.context import get_active_session

# Get the active Snowflake session
session = get_active_session()

# Read data from the Snowflake table
SalesStoreData = pd.read_snowflake('SALES_STORE')
SalesStoreData = pd.to_pandas(SalesStoreData)

# Check for null values in the 'ID' column (assumed to be the first column)
if SalesStoreData.columns[0] == 'ID':  # Ensure the first column is 'ID'
    null_count = SalesStoreData['ID'].isnull().sum()
    if null_count > 0:
        print(f"The 'ID' column contains {null_count} null values.")
    else:
        print("The 'ID' column does not contain any null values.")
else:
    print(f"The first column is not 'ID', it is: {SalesStoreData.columns[0]}")
