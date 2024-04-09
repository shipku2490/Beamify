import pytest

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from your_module import BlockchainDataModel, Config, default_args, blockchain_params, fetch_aave_data, write_to_csv, write_to_database

@pytest.fixture
def mock_requests_get(mocker):
    mock = mocker.patch('requests.get')
    mock.return_value.json.return_value = [{'id': '1', 'timestamp': '2022-01-01', 'priceInEth': 123.45}]
    return mock

def test_fetch_aave_data(mock_requests_get):
    # Test the output of the `fetch_aave_data` function
    data = fetch_aave_data(blockchain_params[0])
    assert isinstance(data, list)
    assert isinstance(data[0], BlockchainDataModel)
    assert data[0].id == '1'
    assert data[0].timestamp == '2022-01-01'
    assert data[0].priceInEth == 123.45

def test_read_csv(tmpdir):
    # Test the output of the `write_to_csv` function
    data = [BlockchainDataModel(id='1', timestamp='2022-01-01', priceInEth=123.45)]
    filename = tmpdir.join('test.csv')
    read_csv(data, filename)
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert rows == [{'id': '1', 'timestamp': '2022-01-01', 'priceInEth': '123.45'}]

def test_store_data_in_postgres(mocker):
    # Test the output of the `write_to_database` function
    data = [BlockchainDataModel(id='1', timestamp='2022-01-01', priceInEth=123.45)]
    mock_conn = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_conn.cursor.return_value = mock_cursor
    store_data_in_postgres(data, mock_conn)
    mock_cursor.execute.assert_called_with("INSERT INTO blockchain_data (id, timestamp, price_in_eth) VALUES (%s, %s, %s)", ('1', '2022-01-01', 123.45))
    mock_conn.commit.assert_called_once()

