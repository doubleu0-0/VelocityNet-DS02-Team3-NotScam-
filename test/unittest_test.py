import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pytest

# Load environment variables from the .env file
load_dotenv()

@pytest.fixture(scope="module")
def snowflake_session():
    """Fixture to set up a Snowflake session."""
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

    session = Session.builder.configs(connection_parameters).create()
    session.use_database(connection_parameters["database"])
    session.use_schema(connection_parameters["schema"])

    yield session  # Provide the session to tests

    # Clean up if needed (e.g., close session)
    session.close()

def test_sales_store_data(snowflake_session):
    """Test to validate the 'SALES_STORE' table data."""
    # Query the 'SALES_STORE' table
    SalesStoreData = snowflake_session.table('SALES_STORE').to_pandas()

    # Check for null values in the 'BUSINESSENTITYID' column
    if 'BUSINESSENTITYID' in SalesStoreData.columns:
        null_count = SalesStoreData['BUSINESSENTITYID'].isnull().sum()
        assert null_count == 0, f"The 'BUSINESSENTITYID' column contains {null_count} null values."
    else:
        raise AssertionError(f"'BUSINESSENTITYID' column not found in the data. Available columns: {SalesStoreData.columns}")

def test_table_exists(snowflake_session):
    """Test to check if the 'SALES_STORE' table exists."""
    table_exists = snowflake_session.sql("SHOW TABLES LIKE 'SALES_STORE'").count() > 0
    assert table_exists, "The 'SALES_STORE' table does not exist in the database."

def test_sales_store_row_count(snowflake_session):
    """Test to validate the row count in the 'SALES_STORE' table."""
    row_count = snowflake_session.table('SALES_STORE').count()
    assert row_count > 0, "The 'SALES_STORE' table is empty."

def test_businessentityid_unique(snowflake_session):
    """Test to ensure that 'BUSINESSENTITYID' column has unique values."""
    SalesStoreData = snowflake_session.table('SALES_STORE').to_pandas()

    if 'BUSINESSENTITYID' in SalesStoreData.columns:
        unique_count = SalesStoreData['BUSINESSENTITYID'].nunique()
        total_count = len(SalesStoreData)
        assert unique_count == total_count, "'BUSINESSENTITYID' column does not have unique values."
    else:
        raise AssertionError(f"'BUSINESSENTITYID' column not found in the data. Available columns: {SalesStoreData.columns}")

def test_column_presence(snowflake_session):
    """Test to check if all expected columns are present in the 'SALES_STORE' table."""
    expected_columns = {'BUSINESSENTITYID', 'SALESPERSONID', 'DEMOGRAPHICS'}
    SalesStoreData = snowflake_session.table('SALES_STORE').to_pandas()
    missing_columns = expected_columns - set(SalesStoreData.columns)
    assert not missing_columns, f"The following expected columns are missing: {missing_columns}"
