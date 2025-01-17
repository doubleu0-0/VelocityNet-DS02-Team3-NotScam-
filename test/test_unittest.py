from snowflake.snowpark import Session
import os

# Initialize Snowflake session
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}
session = Session.builder.configs(connection_parameters).create()

# Set the active session
session.use_database(connection_parameters["database"])
session.use_schema(connection_parameters["schema"])

# Proceed with your test logic
SalesStoreData = session.table('SALES_STORE').to_pandas()

# Check for null values
if 'ID' in SalesStoreData.columns:
    null_count = SalesStoreData['ID'].isnull().sum()
    print(f"The 'ID' column contains {null_count} null values.")
else:
    print(f"'ID' column not found in the data. Available columns: {SalesStoreData.columns}")
