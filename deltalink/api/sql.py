from datetime import datetime
from io import StringIO
from typing import Annotated

import daft
from daft.sql import SQLCatalog
from daft.unity_catalog import UnityCatalog
from fastapi import APIRouter, Body, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sql_metadata import Parser

from deltalink.core.auth import get_auth
from deltalink.core.util import table_config
from deltalink.dependencies import get_unity

router = APIRouter()
auth = get_auth()


class Query(BaseModel):
    query: str


@router.post("/sql/query", tags=["Query"])
async def send_query(
    query: Annotated[
        Query,
        Body(examples=[{"query": "select * from main.bakehouse.sales_suppliers"}]),
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
    df.explain(True, file=plan_io)

    content = {"data": df.to_pylist(), "plan": plan_io.getvalue()}
    headers = {"X-Processing-Time": str((datetime.now() - start).total_seconds())}

    return JSONResponse(
        content=jsonable_encoder(content),
        headers=headers,
    )
