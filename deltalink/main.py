from fastapi import FastAPI
from fastapi.routing import APIRoute
from starlette.middleware.sessions import SessionMiddleware

from deltalink.api.catalog import router as catalog_router
from deltalink.api.data import router as data_router
from deltalink.api.sql import router as sql_router
from deltalink.api.user import router as user_router
from deltalink.core.auth import get_auth
from deltalink.core.config import settings


def custom_generate_unique_id(route: APIRoute) -> str:
    return f"{route.tags[0]}-{route.name}"

msal_auth = get_auth()

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="""This API aim to provide a unified interface for data access and 
                    management using Unity Catalog. It allows users to query data 
                    using SQL, and interact with the Unity Catalog without a 
                    Databricks Compute.""",
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    generate_unique_id_function=custom_generate_unique_id,
)
app.add_middleware(SessionMiddleware, secret_key=settings.SESSION_KEY)

app.include_router(msal_auth.router)
app.include_router(data_router, prefix=settings.API_V1_STR)
app.include_router(sql_router, prefix=settings.API_V1_STR)
app.include_router(catalog_router, prefix=settings.API_V1_STR)
app.include_router(user_router, prefix=settings.API_V1_STR)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
