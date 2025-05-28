from datetime import datetime
from typing import Annotated, List, Optional
import daft
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse
from daft.unity_catalog import UnityCatalogTable
from fastapi_msal import AuthToken, UserInfo
from pydantic import BaseModel
from sql_metadata import Parser
from deltalink.core.auth import get_auth
from deltalink.core.util import ensure_io_from_tables, table_config
from deltalink.dependencies import get_unity
from daft.sql import SQLCatalog
from daft.unity_catalog import UnityCatalog

from deltalink.types.delta_table import DeltaTable, DeltaTableInfo
from deltalake import write_deltalake
import pandas as pd
from deltalink.core.config import settings
from io import StringIO  

router = APIRouter()
auth = get_auth()


class Query(BaseModel):
    query: str


@router.get("/catalogs", dependencies=[Depends(get_unity)], tags=["Catalog"])
async def get_catalogs():
    unity = await get_unity()
    return unity.list_catalogs()


@router.get("/catalogs/{catalog}/schemas", tags=["Catalog"])
async def get_schemas(catalog: str) -> List[str]:
    unity = await get_unity()
    return unity.list_schemas(catalog)


@router.post("/catalogs/{catalog}/schemas", tags=["Catalog"])
async def create_schema(catalog: str, name: str, comments: str) -> None:
    unity = await get_unity()
    return unity._client.schemas.create(
        catalog_name=catalog, name=name, comment=comments
    )


@router.get("/catalogs/{catalog}/schemas/{schema}/tables", tags=["Catalog"])
async def get_tables(catalog: str, schema: str) -> List[str]:
    unity = await get_unity()
    return unity.list_tables(f"{catalog}.{schema}")


@router.post("/catalogs/{catalog}/schemas/{schema}/tables", tags=["Catalog"])
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


@router.post(
    "/catalogs/{catalog}/schemas/{schema}/tables/{table}/append", tags=["Catalog"]
)
async def load_table(
    catalog: str, schema: str, table: str, values: List[object]
) -> None:
    unity = await get_unity()

    df = pd.DataFrame({"id": [1, 2, 3]})
    cache = ensure_io_from_tables(unity, [f"{catalog}.{schema}.{table}"], operation="READ_WRITE")
    table_config = list(cache)[0]
    storage_options = {
        "SAS_TOKEN": table_config.io_config.azure.sas_token
    }

    return write_deltalake(
        table_config.table_uri, df, mode="append", storage_options=storage_options
    )


@router.post("/query", tags=["Query"])
async def send_query(
    query: Annotated[
        Query,
        Body(
            examples=[
                {
                    "query": "select * from main.demo.forecast_hourly_metric where country_code = 'ca' and city_name = 'toronto'"
                }
            ]
        ),
    ],
    request: Request,
    # user: UserInfo = Depends(auth.scheme),
):
    # token: Optional[AuthToken] = await auth.handler.get_token_from_session(
    #     request=request
    # )
    # if not token or not token.access_token:
    #     raise HTTPException(
    #         status_code=status.HTTP_401_UNAUTHORIZED,
    #         detail="Incorrect jwt token",
    #         headers={"WWW-Authenticate": "Bearer"},
    #     )

    # dbx_token = auth.handler.msal_app()._cca.acquire_token_on_behalf_of(
    #     token.access_token, ["https://azuredatabricks.net//user_impersonation"]
    # )
    start = datetime.now()
    parser = Parser(query.query)
    tables = parser.tables
    q = query.query

    uc_catalog: UnityCatalog = await get_unity()
    catalog_config = table_config(uc_catalog, tables)
    sql_catalog = SQLCatalog(catalog_config)

    df = daft.sql(q, catalog=sql_catalog)
    plan_io = StringIO()
    plan = df.explain(True,file=plan_io)

    content = {"data": df.to_pylist(), "plan": plan_io.getvalue()}
    headers = {"X-Processing-Time": str((datetime.now() - start).total_seconds())}

    return JSONResponse(
        content=jsonable_encoder(content),
        headers=headers,
    )
