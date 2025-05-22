from datetime import datetime
from typing import Annotated, List, Optional
import daft
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse
from daft.unity_catalog import  UnityCatalogTable
from fastapi_msal import AuthToken, UserInfo
from pydantic import BaseModel
from sql_metadata import Parser
from deltalink.core.auth import get_auth
from deltalink.core.util import table_config
from deltalink.dependencies import get_unity
from daft.sql import SQLCatalog

router = APIRouter()
auth = get_auth()

class Query(BaseModel):
    query: str


@router.get("/catalogs",dependencies=[Depends(get_unity)], tags=["Catalog"])
async def get_catalogs():
    unity = await get_unity()
    return unity.list_catalogs()


@router.get("/catalogs/{catalog}/schemas", tags=["Catalog"])
async def get_schemas(catalog: str) -> List[str]:
    unity = await get_unity()
    return unity.list_schemas(catalog)


@router.get("/catalogs/{catalog}/schemas/{schema}/tables", tags=["Catalog"])
async def get_tables(catalog: str, schema: str):
    unity = await get_unity()
    return unity.list_tables(f"{catalog}.{schema}")


@router.get("/catalogs/{catalog}/schemas/{schema}/tables/{table}/load", tags=["Catalog"])
async def load_table(catalog: str, schema: str, table: str):
    unity = await get_unity()
    table: UnityCatalogTable = unity.load_table(f"{catalog}.{schema}.{table}")
    return {
        "uri": table.table_uri,
        "sas": table.io_config.azure.sas_token,
        "status": "LOADED",
    }


@router.post("/query", tags=["Query"])
async def send_query(
    query: Annotated[
        Query,
        Body(
            examples=[
                {
                    "query": "select * from dbx_workspace_poc.demo.forecast_hourly_metric where country_code = 'ca' and city_name = 'toronto'"
                }
            ]
        ),
    ],
    request: Request,
    user: UserInfo = Depends(auth.scheme)
    
):
    token: Optional[AuthToken] = await auth.handler.get_token_from_session(request=request)
    if not token or not token.access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect jwt token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    dbx_token = auth.handler.msal_app()._cca.acquire_token_on_behalf_of(token.access_token,["https://azuredatabricks.net//user_impersonation"])
    start = datetime.now()
    parser = Parser(query.query)
    tables = parser.tables
    q = query.query
   
    catalog_config = table_config(tables)
    catalog = SQLCatalog(catalog_config)

    df = daft.sql(q, catalog=catalog)
    plan = df.explain(True)

    content = {"data": df.to_pylist(), "plan": plan}
    headers = {"X-Processing-Time": str((datetime.now()-start).total_seconds())}

    return JSONResponse(content=jsonable_encoder(content), headers=headers,)

