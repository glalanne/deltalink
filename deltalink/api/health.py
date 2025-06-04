from fastapi import APIRouter

from deltalink.core.auth import get_auth

router = APIRouter()
auth = get_auth()


@router.get(
    "/health",
    summary="Health Check",
    description="Endpoint to check the health of the service.",
    response_description="Health status of the service",
    response_model_exclude_none=True,
    response_model_by_alias=False,
    tags=["Health"],
)
async def health() -> None:
    return {"status": "healthy", "message": "Service is running smoothly."}
