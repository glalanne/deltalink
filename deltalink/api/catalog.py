import unitycatalog
from fastapi import APIRouter, Depends, HTTPException

from deltalink.core.auth import get_auth
from deltalink.core.config import settings
from deltalink.dependencies import get_unity
from deltalink.types.delta_table import DeltaTable, DeltaTableInfo

router = APIRouter()
auth = get_auth()


@router.get(
    "/catalogs",
    summary="List all catalogs",
    description="Retrieve a list of all catalogs available in Unity Catalog.",
    response_model=list[str],
    response_description="A list of catalog names.",
    responses={
        200: {
            "description": "A list of catalog names.",
            "content": {"application/json": {"example": ["catalog1", "catalog2"]}},
        },
        404: {"description": "No catalogs found."},
    },
    dependencies=[Depends(get_unity)],
    tags=["Catalog"],
)
async def get_catalogs():
    unity = await get_unity()
    return unity.list_catalogs()


@router.get(
    "/catalogs/{catalog}/schemas",
    summary="List schemas in a catalog",
    description="Retrieve a list of all schemas in a specified catalog.",
    response_model=list[str],
    response_description="A list of schema names in the specified catalog.",
    responses={
        200: {
            "description": "A list of schema names in the specified catalog.",
            "content": {"application/json": {"example": ["schema1", "schema2"]}},
        },
        404: {"description": "Catalog not found or no schemas available."},
    },
    tags=["Catalog"],
)
async def get_schemas(catalog: str) -> list[str]:
    unity = await get_unity()
    return unity.list_schemas(catalog)


@router.post(
    "/catalogs/{catalog}/schemas",
    summary="Create a schema in a UC catalog",
    description="Create a new schema in the specified UC catalog.",
    response_description="Schema created successfully.",
    responses={
        201: {"description": "Schema created successfully."},
        400: {"description": "Invalid input or schema already exists."},
    },
    tags=["Catalog"],
)
async def create_schema(catalog: str, name: str, comments: str) -> None:
    unity = await get_unity()
    return unity._client.schemas.create(
        catalog_name=catalog, name=name, comment=comments
    )


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/tables",
    summary="List tables in a schema",
    description="Retrieve a list of all tables in a specified schema within a catalog.",
    response_model=list[str],
    response_description="A list of table names in the specified schema.",
    responses={
        200: {
            "description": "A list of table names in the specified schema.",
            "content": {"application/json": {"example": ["table1", "table2"]}},
        },
        404: {"description": "Catalog or schema not found, or no tables available."},
    },
    tags=["Catalog"],
)
async def get_tables(catalog: str, schema: str) -> list[str]:
    unity = await get_unity()
    return unity.list_tables(f"{catalog}.{schema}")


@router.post(
    "/catalogs/{catalog}/schemas/{schema}/tables",
    summary="Create a table in a UC schema",
    description="Create a new table in the specified UC schema.",
    response_model=DeltaTableInfo,
    response_description="Table created successfully.",
    responses={
        201: {"description": "Table created successfully."},
        400: {"description": "Invalid input or table already exists."},
    },
    tags=["Catalog"],
)
async def create_table(catalog: str, schema: str, table: DeltaTable) -> DeltaTableInfo:
    unity = await get_unity()
    return unity._client.tables.create(
        catalog_name=catalog,
        schema_name=schema,
        name=table.name,
        comment=table.comment,
        data_source_format="DELTA",
        properties=table.properties,
        table_type="EXTERNAL",
        columns=table.columns,
        storage_location=f"{settings.STORAGE_LOCATION}/{catalog}/{schema}/{table.name}/",
    )


@router.get(
    "/catalogs/{catalog}/schemas/{schema}/tables/{table_name}",
    summary="Get the table information from UC schema",
    description="Retrieve information about a specific table in Unity Catalog.",
    response_model=DeltaTableInfo,
    response_description="Information about the specified table.",
    responses={
        200: {
            "description": "Information about the specified table.",
            "content": {
                "application/json": {"example": {"name": "table1", "columns": []}}
            },
        },
        404: {"description": "Table not found."},
    },
    tags=["Catalog"],
)
async def get_table_info(
    catalog_name: str, schema_name: str, table_name: str
) -> DeltaTableInfo:
    unity = await get_unity()

    try:
        uc_table = unity._client.tables.retrieve(
            f"{catalog_name}.{schema_name}.{table_name}"
        )

        return uc_table
    except unitycatalog.NotFoundError as e:
        raise HTTPException(status_code=404, detail=e.message) from e

    except Exception as e:  # pragma: no cover
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e!s}"
        ) from e
