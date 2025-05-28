from typing import Dict, Iterable, List, Literal
import daft
from daft.unity_catalog import UnityCatalogTable, UnityCatalog
from fastapi.logger import logger
from fastapi_msal import MSALClientConfig

from deltalink.core.config import Settings

cache: Dict[str, UnityCatalogTable] = dict()


def ensure_io_from_tables(
    catalog: UnityCatalog,
    tables: List[str],
    operation: Literal["READ", "READ_WRITE"] = "READ",
) -> Iterable[UnityCatalogTable]:
    """
    Ensure that the tables are loaded from the catalog and cached.
    This is used to avoid loading the same table multiple times.
    Each load will result to a access token for the table in UC
    """

    for table in tables:
        if not table in cache:
            logger.debug(f"Loading credentials for {table}")
            unity_table: UnityCatalogTable = catalog.load_table(
                table, operation=operation, table_type="MANAGED"
            )
            cache[table] = unity_table
            yield unity_table
        else:
            logger.debug(f"Using cached table for {table}")
            yield cache[table]


def table_config(catalog: UnityCatalog, tables: List[str]) -> Dict[str, daft.DataFrame]:
    """
    Load the tables from the catalog and return a dictionary of DataFrames.
    """

    catalog_config = {}

    uc_tables: List[UnityCatalogTable] = list(ensure_io_from_tables(catalog, tables))

    for uc_table in uc_tables:
        df = daft.read_deltalake(uc_table)
        table_name = f"{uc_table.table_info.catalog_name}.{uc_table.table_info.schema_name}.{uc_table.table_info.name}"
        catalog_config[table_name] = df

    return catalog_config


def get_auth_config(settings: Settings):
    client_config: MSALClientConfig = MSALClientConfig()
    client_config.client_id = settings.CLIENT_ID
    client_config.client_credential = settings.CLIENT_SECRET
    client_config.tenant = settings.TENANT_ID

    return client_config
