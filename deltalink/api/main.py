from datetime import datetime
from typing import Annotated, List
import daft
from fastapi import APIRouter, Body, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from daft.unity_catalog import  UnityCatalogTable
from pydantic import BaseModel
from sql_metadata import Parser
from deltalink.core.util import table_config
from deltalink.dependencies import get_unity
from daft.sql import SQLCatalog

router = APIRouter()

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
def send_query(
    query: Annotated[
        Query,
        Body(
            examples=[
                {
                    "query": "select * from dbx_workspace_poc.demo.forecast_hourly_metric where country_code = 'ca' and city_name = 'toronto'"
                }
            ]
        ),
    ]
):
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

