from typing import Dict, List
import daft
from daft.unity_catalog import UnityCatalogTable, UnityCatalog
from fastapi import logger

cache: Dict[str, UnityCatalogTable] = dict()


def ensure_io_from_tables(catalog: UnityCatalog, tables: List[str]):
    for table in tables:
        if not table in cache:
            logger.debug(f"Loading credentials for {table}")
            unity_table: UnityCatalogTable = catalog.load_table(table)
            cache[table] = unity_table


def table_config(tables: List[str]):
    catalog_config = {}

    ensure_io_from_tables(tables)

    for table in tables:
        d_table = daft.read_deltalake(cache[table])
        catalog_config[table] = d_table

    return catalog_config
