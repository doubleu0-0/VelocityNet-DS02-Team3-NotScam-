import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

# Load environment variables from the .env file
load_dotenv()

def test_sales_store_data():
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

    # Validate that all parameters are loaded
    assert all(connection_parameters.values()), "Some required Snowflake connection parameters are missing!"

    # Create the Snowflake session
    session = Session.builder.configs(connection_parameters).create()

    # Set the active session
    session.use_database(connection_parameters["database"])
    session.use_schema(connection_parameters["schema"])

    # Query the 'SALES_STORE' table
    SalesStoreData = session.table('SALES_STORE').to_pandas()

    # Check for null values in the 'ID' column
    if 'ID' in SalesStoreData.columns:
        null_count = SalesStoreData['BUSINESSENTITYID'].isnull().sum()
        assert null_count == 0, f"The 'BUSINESSENTITYID' column contains {null_count} null values."
    else:
        raise AssertionError(f"'BUSINESSENTITYID' column not found in the data. Available columns: {SalesStoreData.columns}")

