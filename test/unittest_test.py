import pytest
from notebook_app import get_session, parse_xml, clean_sales_store_data
import pandas as pd

@pytest.fixture
def session():
    """Fixture to initialize Snowflake session."""
    return get_session()

def test_parse_xml():
    """Test the parse_xml function."""
    xml_input = "<StoreSurvey><StoreName>Test Store</StoreName></StoreSurvey>"
    expected_output = {"StoreName": "Test Store"}
    assert parse_xml(xml_input) == expected_output

def test_clean_sales_store_data(session, mocker):
    """Test the clean_sales_store_data function."""
    # Mock Snowflake data
    mock_data = pd.DataFrame({
        "DEMOGRAPHICS": [
            "<StoreSurvey><StoreName>Store A</StoreName></StoreSurvey>",
            "<StoreSurvey><StoreName>Store B</StoreName></StoreSurvey>"
        ],
        "BUSINESSENTITYID": [1, 2]
    })
    mocker.patch("modin.pandas.read_snowflake", return_value=mock_data)
    mocker.patch("pandas.concat", return_value=mock_data)
    mocker.patch("Session.write_pandas")

    # Call the clean_sales_store_data function
    result = clean_sales_store_data(session)

    # Verify the cleaned data
    assert "StoreName" in result.columns
    assert result.shape[0] == 2

    missing_columns = expected_columns - set(SalesStoreData.columns)
    assert not missing_columns, f"The following expected columns are missing: {missing_columns}"
