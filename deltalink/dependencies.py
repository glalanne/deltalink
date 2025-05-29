from daft.unity_catalog import UnityCatalog

from deltalink.core.config import settings

_catalog:UnityCatalog = None

async def get_unity():
    global _catalog

    if _catalog is None:
        _catalog = UnityCatalog(
            endpoint=settings.UNITY_ENDPOINT,
            # Authentication can be retrieved from your provider of Unity Catalog
            token=settings.UNITY_TOKEN,
        )

    return _catalog
