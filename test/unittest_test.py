from unittest.mock import patch

import pytest
from notebook_app import clean_sales_store_data, parse_xml


def test_parse_xml():
    xml_input = "<StoreSurvey><StoreName>Test Store</StoreName></StoreSurvey>"
    expected_output = {"StoreName": "Test Store"}
    assert parse_xml(xml_input) == expected_output

@patch('notebook_app.read_sales_store_data')
@patch('notebook_app.write_pandas')
def test_clean_sales_store_data(mock_write_pandas, mock_read_sales_store_data):
    # Mock input data
    mock_data = pd.DataFrame({
        "DEMOGRAPHICS": [
            "<StoreSurvey><StoreName>Store A</StoreName></StoreSurvey>",
            "<StoreSurvey><StoreName>Store B</StoreName></StoreSurvey>"
        ],
        "BUSINESSENTITYID": [1, 2]
    })
    mock_read_sales_store_data.return_value = mock_data
    
    # Mock session
    mock_session = None
    
    # Call the function
    result = clean_sales_store_data(mock_session)
    
    # Assertions
    assert "StoreName" in result.columns
    assert result.shape[0] == 2
    mock_write_pandas.assert_called_once()

