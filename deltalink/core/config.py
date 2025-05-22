
from typing import Literal


from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Use top level .env file
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "DeltaLink"

    CLIENT_ID: str
    CLIENT_SECRET: str
    TENANT_ID: str

    # 60 minutes expiration
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 

    FASTAPI_ENV: Literal["local", "staging", "production"] = "local"

    UNITY_ENDPOINT: str
    UNITY_TOKEN: str | None = None
    RAY_ADRESS: str | None = None
    SESSION_KEY: str = "b669310d-d4e6-4c9a-9714-5532bd7e4404"
    REDIRECT_PATH: str = "/getAToken"
    

settings = Settings()  # type: ignore