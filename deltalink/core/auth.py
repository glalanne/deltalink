from fastapi_msal import MSALAuthorization

from deltalink.core.config import settings
from deltalink.core.util import get_auth_config

msal_auth = MSALAuthorization(client_config=get_auth_config(settings))


def get_auth():
    return msal_auth
