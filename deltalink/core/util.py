from collections.abc import Iterable
from typing import Literal

import daft
from cachetools import TTLCache
from daft.unity_catalog import UnityCatalog, UnityCatalogTable
from fastapi.logger import logger
from fastapi_msal import MSALClientConfig

from deltalink.core.config import Settings

DEFAULT_TIME_TO_LIVE = 60 * 60 - 30  # 1 hour minus 30 seconds

# cache: dict[str, UnityCatalogTable] = {}
cache = TTLCache(
    ttl=DEFAULT_TIME_TO_LIVE, maxsize=10000000
)  # Cache for UnityCatalogTable objects


def ensure_io_from_tables(
    catalog: UnityCatalog,
    tables: list[str],
    operation: Literal["READ", "READ_WRITE"] = "READ",
) -> Iterable[UnityCatalogTable]:
    """
    Ensure that the tables are loaded from the catalog and cached.
    This is used to avoid loading the same table multiple times.
    Each load will result to a access token for the table in UC
    """

    for table in tables:
        if table not in cache:
            logger.debug(f"Loading credentials for {table}")
            unity_table: UnityCatalogTable = catalog.load_table(
                table, operation=operation, table_type="MANAGED"
            )
            cache[table] = unity_table
            yield unity_table
        else:
            logger.debug(f"Using cached table for {table}")
            yield cache[table]


def table_config(catalog: UnityCatalog, tables: list[str]) -> dict[str, daft.DataFrame]:
    """
    Load the tables from the catalog and return a dictionary of DataFrames.
    """

    catalog_config = {}

    uc_tables: list[UnityCatalogTable] = list(ensure_io_from_tables(catalog, tables))

    for uc_table in uc_tables:
        df = daft.read_deltalake(uc_table)
        table_name = f"{uc_table.table_info.catalog_name}.{uc_table.table_info.schema_name}.{uc_table.table_info.name}"  # noqa: E501
        catalog_config[table_name] = df

    return catalog_config


def get_auth_config(settings: Settings):
    client_config: MSALClientConfig = MSALClientConfig()
    client_config.client_id = settings.CLIENT_ID
    client_config.client_credential = settings.CLIENT_SECRET
    client_config.tenant = settings.TENANT_ID

    return client_config
