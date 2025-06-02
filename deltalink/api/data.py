from typing import Any

import pandas as pd
from deltalake import DeltaTable as DeltaLakeTable
from deltalake import write_deltalake
from fastapi import APIRouter, HTTPException, status

from deltalink.core.auth import get_auth
from deltalink.core.util import ensure_io_from_tables
from deltalink.dependencies import get_unity
from deltalink.types.delta_table import (
    DeltaTableDelete,
    DeltaTableInsert,
    DeltaTableMerge,
    DeltaTableOptimization,
    DeltaTableVacuum,
)

router = APIRouter()
auth = get_auth()


@router.post(
    "/data",
    summary="Append data to a Delta table",
    description="""Simulare to SQL INSERT new data to an existing 
                   Delta table in Unity Catalog.""",
    response_description="Data appended successfully.",
    responses={
        204: {"description": "Data appended successfully."},
        400: {"description": "Bad Request - No data provided to append to the table."},
    },
    response_model=None,
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Data"],
)
async def load_table(input: DeltaTableInsert) -> None:
    unity = await get_unity()

    df = pd.DataFrame(input.values)
    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data provided to append to the table.",
        )
    cache = ensure_io_from_tables(
        unity,
        [f"{input.catalog_name}.{input.schema_name}.{input.table_name}"],
        operation="READ_WRITE",
    )
    table_config = next(iter(cache))
    storage_options = {"SAS_TOKEN": table_config.io_config.azure.sas_token}

    write_deltalake(
        table_config.table_uri,
        df,
        mode="append",
        storage_options=storage_options,
        partition_by=input.partition_by if input.partition_by else None,
    )

    return None


@router.patch(
    "/data",
    summary="Merge data into a Delta table, similar to SQL MERGE",
    description="""Similar to SQL UPDATE, it takes the given dataset and updates 
                    existing rows based on a predicateDelta table in Unity Catalog.""",
    response_description="Data merged successfully.",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Data"],
)
async def merge_table(input: DeltaTableMerge) -> None:
    unity = await get_unity()

    df = pd.DataFrame(input.values)
    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data provided to append to the table.",
        )

    cache = ensure_io_from_tables(
        unity,
        [f"{input.catalog_name}.{input.schema_name}.{input.table_name}"],
        operation="READ_WRITE",
    )

    table_config = next(iter(cache))
    storage_options = {"SAS_TOKEN": table_config.io_config.azure.sas_token}

    dt = DeltaLakeTable(table_config.table_uri, storage_options=storage_options)
    dt.merge(  # target data
        source=df,  # source data
        predicate=input.predicate,  # condition for matching rows
        source_alias="source",
        target_alias="target",
    ).when_matched_update(  # conditional statement
        updates=input.updates
    ).execute()

    return None


@router.delete(
    "/data",
    summary="Delete rows from a Delta table",
    description="""Simulare to SQL DELETE, it takes the given dataset and deletes 
                   rows from an existing Delta table in Unity Catalog 
                   based on a predicate.""",
    response_description="Rows deleted successfully.",
    responses={
        204: {"description": "Rows deleted successfully."},
        400: {
            "description": "Bad Request - No data provided to delete from the table."
        },
    },
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Data"],
)
async def merge_delete_table(input: DeltaTableDelete) -> None:
    unity = await get_unity()

    df = pd.DataFrame(input.values)
    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data provided to append to the table.",
        )
    cache = ensure_io_from_tables(
        unity,
        [f"{input.catalog_name}.{input.schema_name}.{input.table_name}"],
        operation="READ_WRITE",
    )
    table_config = next(iter(cache))
    storage_options = {"SAS_TOKEN": table_config.io_config.azure.sas_token}

    dt = DeltaLakeTable(table_config.table_uri, storage_options=storage_options)
    dt.merge(
        source=df,
        predicate=input.predicate,
        source_alias="source",
        target_alias="target",
    ).when_matched_delete(predicate="source.deleted = true").execute()

    return None


@router.post(
    "/data/compact",
    summary="Optimize and compact the Delta table",
    description="""Optimize and compact the 
                   Delta table in Unity Catalog.""",
    response_description="Data compacted successfully.",
    responses={
        200: {"description": "Data compaction details."},
        400: {"description": "Bad Request - No data provided to compact the table."},
    },
    tags=["Data"],
)
async def compact_table(input: DeltaTableOptimization) -> dict[str, Any]:
    unity = await get_unity()

    cache = ensure_io_from_tables(
        unity,
        [f"{input.catalog_name}.{input.schema_name}.{input.table_name}"],
        operation="READ_WRITE",
    )
    table_config = next(iter(cache))
    storage_options = {"SAS_TOKEN": table_config.io_config.azure.sas_token}

    dt = DeltaLakeTable(table_config.table_uri, storage_options=storage_options)
    filters: list[tuple[str, str, str]] | None = None

    if input.partition_filters:
        filters = [(f.column, f.operator, f.value) for f in input.partition_filters]

    res = dt.optimize.compact(partition_filters=filters)
    return res


@router.post(
    "/data/vacuum",
    summary="Vacuum and compact the Delta table",
    description="""The vacuum command deletes all files from storage that are 
                   marked for removal in the transaction log and older than the 
                   retention period which is 7 days by default.""",
    response_description="""The command returns a list of all the files 
                            that are removed from storage""",
    responses={
        200: {"description": "Data vacuum details."},
        400: {"description": "Bad Request - No data provided to compact the table."},
    },
    tags=["Data"],
)
async def vacuum_table(input: DeltaTableVacuum) -> list[str]:
    unity = await get_unity()

    cache = ensure_io_from_tables(
        unity,
        [f"{input.catalog_name}.{input.schema_name}.{input.table_name}"],
        operation="READ_WRITE",
    )
    table_config = next(iter(cache))
    storage_options = {"SAS_TOKEN": table_config.io_config.azure.sas_token}

    dt = DeltaLakeTable(table_config.table_uri, storage_options=storage_options)
    res = dt.vacuum(
        retention_hours=input.retention_hours,
        dry_run=input.dry_run,
        enforce_retention_duration=input.enforce_retention_duration,
    )
    return res
