from fastapi import Depends, FastAPI
from fastapi.routing import APIRoute
from fastapi_msal import MSALAuthorization, UserInfo
from starlette.middleware.sessions import SessionMiddleware
from deltalink.api.main import router as api_router
from deltalink.core.auth import get_auth
from deltalink.core.config import settings
from deltalink.core.util import get_auth_config


def custom_generate_unique_id(route: APIRoute) -> str:
    return f"{route.tags[0]}-{route.name}"

msal_auth = get_auth()

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    generate_unique_id_function=custom_generate_unique_id,
)
app.add_middleware(SessionMiddleware, secret_key=settings.SESSION_KEY)

app.include_router(msal_auth.router)
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get(
    "/users/me",
    response_model=UserInfo,
    response_model_exclude_none=True,
    response_model_by_alias=False,
    tags=["Auth"],
)
async def read_users_me(current_user: UserInfo = Depends(msal_auth.scheme)) -> UserInfo:
    return current_user


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
