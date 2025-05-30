from unittest.mock import MagicMock, patch

import pytest

from deltalink.core.util import table_config

# deltalink/core/test_util.py


class DummyTableInfo:
    def __init__(self, catalog_name, schema_name, name):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.name = name


class DummyUnityCatalogTable:
    def __init__(self, catalog_name, schema_name, name):
        self.table_info = DummyTableInfo(catalog_name, schema_name, name)


@pytest.fixture
def mock_catalog():
    catalog = MagicMock()

    def load_table(name, operation=None, table_type=None):
        return DummyUnityCatalogTable("cat", "sch", name)

    catalog.load_table.side_effect = load_table
    return catalog


@patch("deltalink.core.util.daft.read_deltalake")
def test_table_config_returns_dict(mock_read, mock_catalog):
    mock_read.return_value = "dummy_df"
    tables = ["table1", "table2"]
    result = table_config(mock_catalog, tables)
    assert isinstance(result, dict)
    assert len(result) == 2
    for t in tables:
        key = f"cat.sch.{t}"
        assert key in result
        assert result[key] == "dummy_df"


@patch("deltalink.core.util.daft.read_deltalake")
def test_table_config_empty_tables(mock_read, mock_catalog):
    mock_read.return_value = "dummy_df"
    result = table_config(mock_catalog, [])
    assert result == {}
