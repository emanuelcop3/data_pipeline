import pytest
from data_pipeline.clickhouse_client import get_client, execute_sql_script, insert_dataframe
import pandas as pd

def test_get_client():
    client = get_client()
    assert client is not None

def test_execute_sql_script(tmp_path):
    script_path = tmp_path / "test_script.sql"
    script_path.write_text("CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = Memory;")
    client = execute_sql_script(str(script_path))
    result = client.command("SHOW TABLES LIKE 'test_table'")
    assert result == ['test_table']

def test_insert_dataframe():
    client = get_client()
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    insert_dataframe(client, 'test_table', df)
    result = client.query("SELECT * FROM test_table")
    assert len(result) == 3
