from fastapi import APIRouter, Depends
from fastapi_msal import UserInfo

from deltalink.core.auth import get_auth

router = APIRouter()
auth = get_auth()


@router.get(
    "/users/me",
    response_model=UserInfo,
    response_model_exclude_none=True,
    response_model_by_alias=False,
    tags=["Auth"],
)
async def read_users_me(current_user: UserInfo = Depends(auth.scheme)) -> UserInfo:  # noqa: B008
    return current_user
