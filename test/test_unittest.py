from dotenv import load_dotenv
import os
from snowflake.snowpark import Session

# Load environment variables from the .env file
load_dotenv()

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

# Debug: Print the loaded environment variables
print("Loaded Connection Parameters:")
for key, value in connection_parameters.items():
    if key != "password":  # Mask the password for security
        print(f"{key}: {value}")
    else:
        print(f"{key}: ********")

# Validate that all parameters are loaded
if not all(connection_parameters.values()):
    raise ValueError("Some required Snowflake connection parameters are missing!")

# Create the Snowflake session
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

